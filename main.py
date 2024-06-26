# Licensed Materials - Property of IBM
# 5737-M66, 5900-AAA, 5900-A0N, 5725-S86, 5737-I75
# (C) Copyright IBM Corp. 2020, 2022 All Rights Reserved.
# US Government Users Restricted Rights - Use, duplication, or disclosure
# restricted by GSA ADP Schedule Contract with IBM Corp.

import argparse
import logging.config
import os
import sys
import json
import datetime as dt
import gzip
import inspect
import logging
import os
import re
import inquirer

import traceback
import warnings
from collections import defaultdict
from functools import partial
from pathlib import Path
import numpy as np
import pandas as pd
import pandas.tseries.offsets

import threading
import time

from iotfunctions.util import setup_logging

from pipeline import catalog
from pipeline import dag as pipeline
from pipeline import util
from pipeline import web
from pipeline import deploy

logger = logging.getLogger(__name__)

TRIRIGA_BUILDING = 'TRIRIGA_BUILDING'
TRIRIGA_FLOOR = 'TRIRIGA_FLOOR'
TRIRIGA_SPACE = 'TRIRIGA_SPACE'
LOCATION_TYPE = 'locationTypeName'

hostName = "localhost"
serverPort = 8080

MAIN_LOGFILE_NAME = 'main.log'
##################################################################################################
# Set production_mode to False to suppress any output of pipeline into database or ObjectStorage
##################################################################################################
production_mode = True

exit_code = -1
log_level = logging.WARNING
service = None

parser = argparse.ArgumentParser()
parser.add_argument('--tenant-id', help='Tenant ID')
parser.add_argument('--log-level', choices=['debug', 'info', 'warning', 'error', 'critical'], default='warning', help='Log level')
parser.add_argument('--render', help='Render pipeline as DAG', nargs='?', const=True, default=False)
parser.add_argument('--deploy', help='Deploy space planning functions to all spaces in a floor - optional argument to search for location', nargs='?', const=True, default=False)
parser.add_argument('--uuid', help='Explicitly specify UUID')
parser.add_argument('--td', nargs='?', help='Generate top-down DAG instead of left-right', const=True, default=False)
parser.add_argument('--site', help='Site')
parser.add_argument('--dumb', help='No terminal capabilities', nargs='?', const=True, default=False)
args, unknown = parser.parse_known_args()

#from .catalog import Catalog, CATEGORY_AGGREGATOR, CATEGORY_TRANSFORMER
#from .util import TimeZones, DIM_TIME_ZONE, copy_timezone_enabled_df


def start_web():
    global webServer
    print("Server started http://%s:%s" % (hostName, serverPort))
    webServer.serve_forever()


if args.tenant_id is None or args.site is None:
    print('Argument --tenant-id and --site must be given.')
    os._exit(os.EX_USAGE)

# read credentials
with open('credentials.json', encoding='utf-8') as F:
    credentials = json.loads(F.read())

count = 0
if 'baseurl' in credentials:
    os.environ['API_BASEURL'] = credentials['baseurl']
    count += 1
if 'apikey' in credentials:
    os.environ['API_KEY'] = credentials['apikey']
    count += 1
if 'apitoken' in credentials:
    os.environ['API_TOKEN'] = credentials['apitoken']
    count += 1
if count < 3: 
    print('Credentials file missing or incomplete - requires field baseurl, apikey, apitoken')
    os._exit(os.EX_USAGE)

os.environ['DB_CONNECTION_STRING']=''
os.environ['DB_TYPE']='DB2'
os.environ['TENANT_ID']=args.tenant_id

# set global variables for util
util.setup_credentials(api_baseurl=credentials['baseurl'], api_key=credentials['apikey'], \
                       api_token=credentials['apitoken'], tenant_id=args.tenant_id)


if args.log_level is not None:
    if args.log_level == 'debug':
        log_level = logging.DEBUG
    elif args.log_level == 'info':
        log_level = logging.INFO
    elif args.log_level == 'warning':
        log_level = logging.WARNING
    elif args.log_level == 'error':
        log_level = logging.ERROR
    elif args.log_level == 'critical':
        log_level = logging.CRITICAL

if args.dumb is None:
    args.dumb = False

setup_logging(log_level, filename=MAIN_LOGFILE_NAME)
logger = logging.getLogger('monitor-pipeline-utils')
logger.info(vars(args))

try:
    #print('H______', type(args.render))
    if args.render or args.deploy:

        location_search = None
        site = args.site
        #print('HERE', site)

        try:
            sitemeta_res = util.api_request('/api/v2/core/sites/search', method='post', json={"search": site}, timeout=60)
            sitemeta = sitemeta_res.json()
        except Exception as e:
            print(e)
        #print("HERE2", sitemeta)

        if args.uuid is None:
            try:
                locations_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations', timeout=60)
                locations_json = locations_res.json()
            except Exception as e:
                print(e)

            # iterate over locations to strip list from elements without derived metrics
            #for loc in locations['results']:

            locations = locations_json['results']
            #print('HERE3', type(locations))

            if isinstance(args.render, str):
                location_search = args.render

            if isinstance(args.deploy, str):
                location_search = args.deploy

            if location_search is not None:
                temp = re.compile(location_search)
                locations_filt = [x for x in locations if temp.search(x['alias']) is not None]
                #print('HERE3a', locations_filt)
                locations = locations_filt
                # expand the first location found
                depth = 0
                systems = None
                if len(locations) > 0:
                    try:
                        systems_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[0]['uuid'] + '/systems', timeout=60)
                        systems = systems_res.json()
                        #print('HO', systems)
                    except Exception as e:
                        print(str(e))

                for loc in locations: loc['depth'] = 0

                idx = 0
                while idx < len(locations):

                    # limited nesting
                    if locations[idx]['depth'] > 1:
                        idx += 1
                        continue

                    try:
                        locations_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + \
                            '/systems/' + systems[0]['uuid'] + '/locations/' + locations[idx]['uuid'] + '/sublocations', timeout=60)
                        locations_res = locations_res.json()['results']
                    except Exception as e:
                        print('No sublocations' + str(e))

                    #print('SUB', idx, len(locations_res))
                    # set depth and insert right after index
                    for loc in locations_res: loc['depth'] = locations[idx]['depth'] + 1 

                    # insert by slicing
                    '''
                    if idx+1 < len(locations):
                        print('INSERT BETWEEN', locations[idx]['alias'], locations[idx+1]['alias'])
                    else:
                        print('INSERT AFTER', locations[idx]['alias'])
                    '''
                    locations[idx+1:idx+1] = locations_res

                    # increase index - cover sublocations, too
                    idx += 1

            locnames = [(entry['name'] + '/' + entry['alias'], idx) for idx,entry in enumerate(locations)]

            #if not isinstance(args.render, str):
            if location_search is None:
                locnames = sorted(locnames, key=lambda x: x[0])

            # 14998506
            #print("HERE4", locnames[0])

        if args.render:

            thread = None
            webServer = web.MyServer((hostName, serverPort), web.MyHandlerMermaid, None, None, None)

            while True:

                #6eb3688d-64eb-4320-87e6-dcbdc0dbc1b5
                uuid = None
                location = {'alias': ''}
                if args.uuid is None and not args.dumb:
                    questions = [ inquirer.List( "location", message="Select location from list", choices=locnames) ]

                    answers = inquirer.prompt(questions)
                    loc_idx = answers['location']
                    uuid = locations[loc_idx]['uuid']
                    location['alias'] = locations[loc_idx]['alias']

                    print("HERE5", loc_idx)
                elif args.uuid is None and args.dumb:
                    loc_idx = 0
                    uuid = locations[loc_idx]['uuid']
                    location['alias'] = locations[loc_idx]['alias']
                else:
                    uuid = args.uuid
                    location['alias'] = 'NotKnown'

                try:
                    service = pipeline.PipelineReader(args.tenant_id, uuid, log_level=log_level)
                    print('Here10')

                    diagram = service.render_kpi_pipelines(topdown=args.td)
                    print(args.td)

                    if diagram is None and args.uuid is None:
                        #print('REMOVE')
                        locnames.remove(loc_idx)

                    webServer.renderCmd = diagram
                    webServer.entityType = uuid
                    webServer.location = location

                    if args.uuid is not None:
                        webServer.serve_forever()
                    elif thread is None:
                        try:
                            thread = threading.Thread(target = start_web)
                            thread.start()
                        except Exception as te:
                            print('starting thread went wrong', str(te))

                except Exception as e:
                    print('something went wrong', str(e))
                    raise Exception()

        if args.deploy:

            # function definitions to deploy to all sublocation spaces

            loc_idx = None

            if args.dumb:
                loc_idx = 0
            else:
                questions = [ inquirer.List( "location", message="Select location from list", choices=locnames) ]

                answers = inquirer.prompt(questions)
                loc_idx = answers['location']

            deploy.deploy_building(locations, loc_idx, sitemeta)

    logger.info('Execution was successful.')
    exit_code = 0
except Exception as ex:
    logger.warning('Execution failed.')
    exit_code = -1
finally:
    if service:
        #service.release_resource()
        service = None

# force flushing log file
logging.shutdown()

sys.exit(exit_code)
