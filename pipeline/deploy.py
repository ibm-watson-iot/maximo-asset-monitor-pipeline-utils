# *****************************************************************************
# © Copyright IBM Corp. 2018, 2024  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************
#
# Helper functions to deploy KPI functions to a Tririga location hierarchy
#

import logging.config
import os
import random
import re
import string
import importlib
import json

import pandas as pd
import pandas.tseries.frequencies
import requests

from pipeline import util

TRIRIGA_BUILDING = 'TRIRIGA_BUILDING'
TRIRIGA_FLOOR = 'TRIRIGA_FLOOR'
TRIRIGA_SPACE = 'TRIRIGA_SPACE'
LOCATION_TYPE = 'locationTypeName'

logger = logging.getLogger(__name__)

# locations - list of locations
# loc_idx   - index of the floor with all spaces to deploy the KPI functions to
#   returns - number of locations equipped with KPI functions
def deploy_space(locations, loc_idx, sitemeta):
    # function definitions to deploy to all sublocation spaces
    kpi_pe_def = '{"description":"","catalogFunctionName":"PythonExpression","enabled":true,"execStatus":false,"granularityName":null,"output":{"output_name":"OccupSpace_rollavg_15min"},"outputMeta":{"OccupSpace_rollavg_15min":{"transient":false}},"input":{"expression":"sp.signal.savgol_filter(df[\\"Space-OccupancyCount_Minute\\"],15,0)"},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":null,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"PythonExpression","status":"ACTIVE","description":"","learnMore":null,"moduleAndTargetName":"iotfunctions.bif.PythonExpression","category":"TRANSFORMER","output":[{"name":"output_name","description":"Output of expression","learnMore":null,"type":null,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"jsonSchema":null,"cardinalityFrom":null,"tags":[]}],"input":[{"name":"expression","description":"Define alert expression using pandas syntax. Example: df[inlet_temperature]>50","learnMore":null,"type":"CONSTANT","required":true,"dataType":"LITERAL","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":["TEXT","EXPRESSION"]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'
    kpi_agg_def = '{"tenantId":"main","description":"","catalogFunctionName":"AggregateWithExpression","enabled":true,"execStatus":false,"granularityName":"Daily","output":{"name":["Daily_max_15minwin"]},"outputMeta":{"Daily_max_15minwin":{"dataType":"NUMBER","transient":false}},"input":{"expression":"np.round(x.max())","source":["OccupSpace_rollavg_15min"]},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":14393,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"AggregateWithExpression","status":"ACTIVE","description":"","learnMore":null,"moduleAndTargetName":"iotfunctions.bif.AggregateWithExpression","category":"AGGREGATOR","output":[{"name":"name","description":"Choose the data items that you would like to aggregate","learnMore":null,"type":null,"dataType":null,"dataTypeFrom":"source","dataTypeForArray":null,"jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","items":{"type":"string"}},"cardinalityFrom":"source","tags":[]}],"input":[{"name":"source","description":"Choose the data items that you would like to aggregate","learnMore":null,"type":"DATA_ITEM","required":true,"dataType":"ARRAY","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","minItems":"1","items":{"type":"string"}},"defaultValue":null,"tags":[]},{"name":"expression","description":"Paste in or type an AS expression","learnMore":null,"type":"CONSTANT","required":true,"dataType":"LITERAL","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":["TEXT","EXPRESSION"]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'

    my_depth = locations[loc_idx]['depth']

    logger.info('Deploy to Space: ' + str(locations[loc_idx]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='post', json=json.loads(kpi_pe_def), timeout=60)
        print('Created Transformer result:', post_res)
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='post', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)


# locations - list of locations
# loc_idx   - index of the floor to deploy the KPI functions to
# sitemeta  - site metadata for REST call
def deploy_floor(locations, loc_idx, sitemeta):

    # function definitions to deploy to all Floors
    kpi_agg_def = '{"tenantId": "main", "name": "fc668a27-f4b1-4002-9073-526bba3800b2", "description": "Calculates the sum of the values in your data set.", "catalogFunctionName": "Sum", "enabled": true, "execStatus": false, "granularityName": "Daily", "output": {"name": "Daily_max_occup_agg"}, "outputMeta": {"Daily_max_occup_agg": {"transient": false}}, "input": {"min_count": 0, "source": "Daily_max_15minwin"}, "inputMeta": null, "scope": null, "schedule": {}, "backtrack": {}, "granularitySetId": 14390, "catalogFunctionVersion": null, "catalogFunctionDto": {"name": "Sum", "status": "ACTIVE", "description": "Calculates the sum of the values in your data set.", "learnMore": null, "moduleAndTargetName": "iotfunctions.aggregate.Sum", "category": "AGGREGATOR", "output": [{"name": "name", "description": "Enter a name for the data item that is produced as a result of this calculation.", "learnMore": null, "type": null, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "jsonSchema": null, "cardinalityFrom": null, "tags": []}], "input": [{"name": "source", "description": "Select the data item that you want to use as input for your calculation.", "learnMore": null, "type": "DATA_ITEM", "required": true, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "values": null, "jsonSchema": null, "defaultValue": null, "tags": []}, {"name": "min_count", "description": "The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.", "learnMore": null, "type": "CONSTANT", "required": false, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "values": null, "jsonSchema": null, "defaultValue": null, "tags": []}], "url": null, "tags": [], "incrementalUpdate": null, "image": null, "version": "V1"}}'

    my_depth = locations[loc_idx]['depth']

    logger.info('Deploy to Floor: ' + str(locations[loc_idx]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='post', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)



# locations - list of locations
# loc_idx   - index of the building to deploy the KPI functions to
# sitemeta  - site metadata for REST call
def deploy_building(locations, loc_idx, sitemeta):

    # function definitions to deploy to all Building
    kpi_agg_def = '{"tenantId":"main","name":"26628bbf-356e-4655-a727-2b28417a1e89","description":"Calculates the sum of the values in your data set.","catalogFunctionName":"Sum","enabled":true,"execStatus":false,"granularityName":"Daily","output":{"name":"Daily_occupmax_building_level"},"outputMeta":{"Daily_occupmax_building_level":{"transient":false}},"input":{"min_count":0,"source":"Daily_max_occup_agg"},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":13991,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"Sum","status":"ACTIVE","description":"Calculates the sum of the values in your data set.","learnMore":null,"moduleAndTargetName":"iotfunctions.aggregate.Sum","category":"AGGREGATOR","output":[{"name":"name","description":"Enter a name for the data item that is produced as a result of this calculation.","learnMore":null,"type":null,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"jsonSchema":null,"cardinalityFrom":null,"tags":[]}],"input":[{"name":"source","description":"Select the data item that you want to use as input for your calculation.","learnMore":null,"type":"DATA_ITEM","required":true,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":[]},{"name":"min_count","description":"The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.","learnMore":null,"type":"CONSTANT","required":false,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":[]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'

    my_depth = locations[loc_idx]['depth']

    i = loc_idx

    logger.info('Deploy to Building: ' + str(locations[i]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[i]['uuid'] + '/kpiFunctions',
                                         method='post', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)

    logger.info('Proceed to lower levels')

    i += 1
    while i < len(locations) and locations[i]['depth'] > my_depth:

        if locations[i][LOCATION_TYPE] == TRIRIGA_FLOOR:
            deploy_floor(locations, i, sitemeta)
        elif locations[i][LOCATION_TYPE] == TRIRIGA_SPACE:
            deploy_space(locations, i, sitemeta)
        i += 1

    logger.info('B: Covered ' + str(i - loc_idx) + ' locations')

