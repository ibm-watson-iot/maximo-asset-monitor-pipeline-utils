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
# Main class PipelineReader renders the engine input for a specific entity or asset type
#  as directed acyclic graph (DAG) for mermaidjs
#

import argparse
import logging.config
import os
import sys
import json
import datetime as dt
import time
import gzip
import inspect
import logging
import os
import re

import traceback
import warnings
from collections import defaultdict
from functools import partial
from pathlib import Path
import numpy as np
import pandas as pd
import pandas.tseries.offsets


from iotfunctions import dbhelper, dbtables, aggregate, loader, util as iot_util
#from iotfunctions.util import setup_logging
#from iotfunctions.aggregate import Aggregation
from iotfunctions.db import Database
from iotfunctions.dbhelper import check_sql_injection, check_sql_injection_extended
#from iotfunctions.metadata import EntityType
#from iotfunctions.pipeline import CalcPipeline
#from iotfunctions.stages import ProduceAlerts, PersistColumns
#from iotfunctions.util import get_fn_expression_args, get_fn_scope_sources, log_data_frame, UNIQUE_EXTENSION_LABEL, \
#    rollback_to_interval_boundary, get_max_frequency, get_backtrack_from_frequency
from iotfunctions.util import get_fn_expression_args, get_fn_scope_sources

from pipeline import catalog
from pipeline import util


DATA_ITEM_NAME_KEY = 'name'
DATA_ITEM_TYPE_KEY = 'type'
DATA_ITEM_COLUMN_NAME_KEY = 'columnName'

CATALOG_FUNCTION_NAME_KEY = 'name'

KPI_FUNCTION_NAME_KEY = 'name'
KPI_FUNCTION_FUNCTIONNAME_KEY = 'catalogFunctionName'
KPI_FUNCTION_FUNCTION_ID_KEY = 'kpiFunctionId'
#KPI_FUNCTION_ENABLED_KEY = 'enabled'
KPI_FUNCTION_GRANULARITY_KEY = 'granularityName'
KPI_FUNCTION_INPUT_KEY = 'input'
KPI_FUNCTION_OUTPUT_KEY = 'output'
KPI_FUNCTION_SCOPE_KEY = 'scope'
FUNCTION_PARAM_NAME_KEY = 'name'
FUNCTION_PARAM_TYPE_KEY = 'type'
AGGREGATOR_INPUT_SOURCE_KEY = 'source'
AGGREGATOR_OUTPUT_SOURCE_KEY = 'name'

# Currently, all event and dimension tables are expected to store the entity ID in column DEVICEID (as VARCHAR)
ENTITY_ID_COLUMN = 'deviceid'
ENTITY_ID_NAME = 'ENTITY_ID'
DATAFRAME_INDEX_ENTITYID = 'id'
DEFAULT_DATAFRAME_INDEX_TIMESTAMP = 'timestamp'

DATA_ITEM_TYPE_DIMENSION = 'DIMENSION'
DATA_ITEM_TYPE_EVENT = 'EVENT'
DATA_ITEM_TYPE_METRIC = 'METRIC'


logger = logging.getLogger(__name__)


ENGINE_INPUT_REQUEST_TEMPLATE = '/api/v2/core/pipeline/{0}/input'
GRANULARITY_REQUEST_TEMPLATE = '/api/granularity/v1/{0}/entityType/{1}/granularitySet'
KPI_FUNCTION_REQUEST_TEMPLATE = '/api/kpi/v1/{0}/entityType/{1}/kpiFunction'
GRANULARITY_SETSTATUS_TEMPLATE = '/api/v2/core/deviceTypes/{0}/granularity?status=true'

def get_kpi_targets(kpi):
    targets = list()

    for key in kpi.get(KPI_FUNCTION_OUTPUT_KEY, []):
        target = kpi[KPI_FUNCTION_OUTPUT_KEY][key]
        if isinstance(target, str):
            # targets.append(target)
            targets.extend([t.strip() for t in target.split(',') if len(t.strip()) > 0])
        elif isinstance(target, list) and all([isinstance(t, str) for t in target]):
            targets.extend(target)

    return targets


def get_all_kpi_targets(pipeline):
    targets = set()

    for kpi in pipeline:
        targets.update(get_kpi_targets(kpi))

    return targets


class Grains(defaultdict):

    def __init__(self, granularities):
        self.granularities = granularities

    def get_granularity(self, grain_name):
        if grain_name is None:
            return self.get(None)
        elif grain_name not in self.granularities:
            return None

        return self.get(self.granularities[grain_name])

class DataItemMetadata(): 
        
    def __init__(self):
        self.data_items = dict()
                                     
    def add(self, data_item):
        self.data_items[data_item[DATA_ITEM_NAME_KEY]] = data_item
                    
    def get_all_names(self):
        return {v[DATA_ITEM_NAME_KEY] for v in self.data_items.values()}
                        
    def get(self, name):
        return self.data_items.get(name, None)

    def get_names(self, type):
        return {v[DATA_ITEM_NAME_KEY] for v in self.data_items.values() if v[DATA_ITEM_TYPE_KEY] in util.asList(type)}

    def get_column_2_data_dict(self):
        return {v['columnName'].lower(): k for k, v in self.data_items.items()}
            
    def get_dimensions(self):
        return self.get_names([DATA_ITEM_TYPE_DIMENSION])

    def get_raw_metrics(self):
        return self.get_names([DATA_ITEM_TYPE_METRIC, DATA_ITEM_TYPE_EVENT, DATA_ITEM_TYPE_DIMENSION])

    def get_derived_metrics(self):
        return self.get_all_names() - self.get_raw_metrics()

    def __iter__(self):
        return DataItemMetadataIter(self)


class KpiTreeNode(object):
    def __init__(self, name, kpi, dependency=None, level=None):
        self.name = name
        self.kpi = kpi
        if dependency is None or isinstance(dependency, list):
            self.dependency = dependency
        else:
            self.dependency = [dependency]
        self._level = level
        self.children = set()

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name

    def tree_level(self):
        if self._level is not None or self.dependency is None:
            return self._level
        elif self.dependency and len(self.dependency) > 0:
            return max([d.tree_level() if isinstance(d, KpiTreeNode) else 0 for d in self.dependency]) + 1
        else:
            return 1

    def __repr__(self):
        if self.dependency is not None:
            return '(%s) %s <- %s' % (
            self.tree_level(), self.name, str(['(%s) %s' % (dep.tree_level(), dep.name) for dep in self.dependency]))
        else:
            return '%s' % (self.name)

    def get_all_dependencies(self):
        dependencies = set()

        if self.dependency is None:
            return dependencies

        queue = self.dependency.copy()
        while len(queue) > 0:
            treenode = queue.pop(0)
            dependencies.add(treenode)
            if treenode.dependency is not None and len(treenode.dependency) > 0:
                queue.extend(treenode.dependency)

        return dependencies

    def get_all_descendants(self):
        descendants = set()
        queue = list(self.children)
        while len(queue) > 0:
            treenode = queue.pop(0)
            descendants.add(treenode)
            if len(treenode.children) > 0:
                queue.extend(treenode.children)

        return descendants


class PipelineReader:
    def __init__(self, tenant_id, uuid, log_level=None):
        try:
            self.logger_name = '%s.%s' % (self.__module__, self.__class__.__name__)
            self.logger = logging.getLogger(self.logger_name)

            # Determine launch date: precision must not be smaller than seconds because launch date is stored in
            # database (Checkpoint table) and some databases are configured not to accept fractions of a second.
            self.launch_date = pd.Timestamp.utcnow().tz_convert(tz=None) - pd.DateOffset(microsecond=0, nanosecond=0)
            logger.info(f"launch_date = {self.launch_date}")

            # Set root logger to required log level explicitly. Otherwise the log level of the calling environment is active.
            if log_level is not None:
                logging.getLogger().setLevel(log_level)

            self.tenant_id = tenant_id
            self.catalog = catalog.Catalog()
            self.granularities = dict()
            self.granularities_local = dict()
            self.pipeline = list()
            self.pipeline_local = list()

            # dict of target data item name to list of grain dimensions
            # mainly used when persiting to KPI tables, need to know the grain dimensions
            self.target_grains = defaultdict(list)
            self.target_grains_local = defaultdict(list)
            self.checkpoints_to_upsert = defaultdict(dict)

            # create an iotfunctions database connection and entity_type

            print("PREP")
            meta = dict()
            meta['name'] = 0
            meta['schema'] = 'None'
            #uuid = 'b9a733ff-506c-427a-a593-d6a6b07e81d9'
            meta['uuid'] = uuid
            # no db required
            os.environ["DB_TYPE"] = "SQLITE"   # force dummy
            credentials = {'as': {'host': os.getenv('API_BASEURL'), 'api_key': os.getenv('API_KEY'), 'api_token': os.getenv('API_TOKEN')}, 'sqlite': 'test'}
            self.db = Database(tenant_id=tenant_id, credentials=credentials, entity_type_id=0, entity_metadata = meta)
            self.db_type = self.db.db_type

            self.is_postgre_sql = False if self.db_type == 'db2' else True
            self.db_connection = self.db.native_connection
            self.db_connection_dbi = self.db.native_connection_dbi
            self.model_store = self.db.model_store

            self._init_service(uuid=uuid)

            self.catalog.load_custom_functions(catalog_functions=self._entity_catalog_functions)
            kwargs = {'logical_name': self.entity_type, '_timestamp': self.eventTimestampName,
                      '_timestamp_col': self.eventTimestampColumn, '_dimension_table_name': self.dimensionTable,
                      '_entity_type_id': 0, '_db_connection_dbi': self.db_connection_dbi,
                      '_db_schema': self.schema, '_data_items': self.data_items, 'tenant_id': tenant_id,
                      '_db_connection': self.db_connection}

            #  Constants :: These are arbitrary parameter values provided by the server and copied onto the entity type
            logger.debug('Constants: {constant_name} = {constant_value}')
            for constant in self._entity_constants:
                key = constant['name']
                if isinstance(constant['value'], dict):
                    value = constant['value'].get('value', constant['value'])
                else:
                    value = constant['value']
                kwargs[key] = value
                logger.debug("%20s = %s" % (key, value))

            self.df_dimension = None

            # No timezones defined. Assume UTC for all devices
            tmp = {device: None for device in self.existing_devices}
            self.timezones = util.TimeZones(tmp)

        except BaseException as exception:
            print("ISSUE", exception)
            raise

    def render_kpi_pipelines(self, location=None, topdown=False):

        logger.info("<<render_kpi_pipelines")
        diagram = None
        try:
            all_granularities = self.granularities.copy()
            all_granularities.update(self.granularities_local)
            result_dfs = Grains(all_granularities)

            pipeline = self.get_pipeline(local_only=False)

            if len(pipeline) == 0:
                print("GONE")
                return False

            # parse all KPI configuration to get their dependency tree as a dependency-ordered list
            queue = self.get_kpi_dependency_tree_processing_queue(pipeline)

            # Get last execution time from CHECK_POINT table
            # Be aware: last_execution_time can be None if there is no entry in CHECK_POINT table!
            last_execution_time = pd.Timestamp.utcnow().tz_convert(tz=None) - pd.DateOffset(months=1)  # go back one day

            self.previous_launch_date = last_execution_time
            logger.info(f"previous_launch_date = {self.previous_launch_date}")

            # Calculate Backtrack and handle schedules
            scheduled_and_up = set()
            scheduled_but_not_up = set()
            not_scheduled = set()
            skipped = set()
            not_skipped = set()
            # first, identify those with schedules and whether the schedule is up or not
            for kpi_tree_node in queue:
                schedule = kpi_tree_node.kpi.get('schedule')
                if schedule is not None and len(schedule) > 0:
                    if util.is_schedule_up(batch_start=last_execution_time, batch_end=self.launch_date,
                                           interval_start=schedule.get('starting_at'),
                                           interval=schedule.get('every')):
                        scheduled_and_up.add(kpi_tree_node)
                    else:
                        scheduled_but_not_up.add(kpi_tree_node)
                else:
                    not_scheduled.add(kpi_tree_node)

            self.logger.debug('KPIs without schedule: %s' % str([t.name for t in not_scheduled]))
            self.logger.debug('KPIs with due schedule: %s' % str([t.name for t in scheduled_and_up]))
            self.logger.debug('KPIs with undue schedule: %s' % str([t.name for t in scheduled_but_not_up]))

            # compute dot string
            dot = "digraph {"

            if topdown:
                diagram = "graph TD\n "
            else:
                diagram = "graph LR\n "

            #self.tree_level(), self.name, str(['(%s) %s' % (dep.tree_level(), dep.name) for dep in self.dependency]))
            linkCount = 0
            for q in queue:
                #print(q.kpi)
                catalogFunctionName = None
                try:
                    catalogFunctionName = q.kpi[KPI_FUNCTION_FUNCTIONNAME_KEY]
                except Exception as e:
                    print('EX1', str(e))
                logger.info('Func ', catalogFunctionName)
                if catalogFunctionName == 'ChildDataLoader':
                    for dep in q.kpi[KPI_FUNCTION_INPUT_KEY]['loadedDataItems']:
                        #print('DEPEND', dep)
                        try:
                            diagram += dep['childDataItemName'].replace('--','__') + " -- " + 'CHILDREN:' + dep['aggregationFunction'][KPI_FUNCTION_FUNCTIONNAME_KEY] +\
                                     "--> " + q.name.replace('--','__') + "\n"
                            linkCount += 1
                        except Exception as e:
                            print('EX2', str(e))
                elif catalogFunctionName == 'InternalHierarchySummary':
                    print(q.kpi)
                    '''
                    for dep in q.kpi[KPI_FUNCTION_INPUT_KEY]['loadedDataItems']:
                        #print('DEPEND', dep)
                        try:
                            diagram += dep['childDataItemName'].replace('--','__') + " -- " + 'CHILDREN:' + dep['aggregationFunction']['catalogFunctionName'] +\
                                     "--> " + q.name.replace('--','__') + "\n"
                        except Exception as e:
                            print('EX2', str(e))
                    '''
                else:
                    for dep in q.dependency:
                        dot += dep.name + " -> " + q.name + "[label=" + q.kpi[KPI_FUNCTION_FUNCTIONNAME_KEY] + ",weight=" + str(dep.tree_level()+1) + "];"
                        # -- in names breaks the mermaid syntax
                        diagram += dep.name.replace('--','__') + " -- " + q.kpi[KPI_FUNCTION_FUNCTIONNAME_KEY] + " --> " + q.name.replace('--','__') + "\n"
                        if q.kpi[KPI_FUNCTION_FUNCTIONNAME_KEY] in self.catalog.unavailable_functions:
                            diagram += "linkStyle " + str(linkCount) + " stroke:#ff3,stroke-width:4px,color:red;"
                        linkCount += 1
            dot += "}"
            print(diagram)
            #
        except Exception as e:
            logger.error("Leaving because of " + str(e))

        logger.info(">>render_kpi_pipelines")
        return diagram

    def _set_pipeline(self, pipeline):
        if pipeline is None:
            pipeline = []

        # self.logger.debug('kpi_declarations=%s' % json.dumps(pipeline, sort_keys=True, indent=2))
        self.pipeline = [kpi for kpi in pipeline if kpi.get('enabled', False) == True]
        self.logger.info('kpi_declarations_filtered(enabled)=%s' % self.pipeline)

        self.target_grains = defaultdict(list)

        # replacing/filling grain name with actual grain data items
        for kpi in pipeline:
            if kpi.get(KPI_FUNCTION_GRANULARITY_KEY) is not None:
                gran = self.granularities[kpi[KPI_FUNCTION_GRANULARITY_KEY]]
                kpi[KPI_FUNCTION_GRANULARITY_KEY] = gran

                for target in get_kpi_targets(kpi):
                    self.target_grains[target] = kpi[KPI_FUNCTION_GRANULARITY_KEY]

        self.logger.info('kpi_target_grains=%s' % dict(self.target_grains))


    def get_pipeline(self, local_only=False):
        if local_only:
            all_local_targets = get_all_kpi_targets(self.pipeline_local)

            self.logger.info('all_local_targets=%s' % str(all_local_targets))

            all_local_dependencies = set()
            kpi_tree = self.parse_kpi_dependency_tree(pipeline=(self.pipeline + self.pipeline_local))[0]
            for name, treenode in kpi_tree.items():
                if name in all_local_targets:
                    all_local_dependencies.add(treenode)
                    all_local_dependencies.update(treenode.get_all_dependencies())

            self.logger.info('all_local_dependencies=%s' % str(all_local_dependencies))

            filtered_tree = dict()
            for name, treenode in kpi_tree.items():
                if treenode in all_local_dependencies:
                    filtered_tree[name] = treenode

            self.logger.info('filtered_tree=%s' % str(filtered_tree))

            filtered_names = set(filtered_tree.keys())

            pipeline = self.pipeline_local.copy()
            for kpi in self.pipeline:
                targets = get_kpi_targets(kpi)
                if any([t in filtered_names for t in targets]):
                    pipeline.append(kpi)

            self.logger.info('pipeline=%s' % str(pipeline))

            return pipeline
        else:
            return self.pipeline + self.pipeline_local

    def _get_engine_input(self, uu_id):
        engine_input_response = util.api_request(ENGINE_INPUT_REQUEST_TEMPLATE.format(uu_id), timeout=60)
        print(engine_input_response, ENGINE_INPUT_REQUEST_TEMPLATE.format(uu_id))
        if engine_input_response is None:
            raise RuntimeError('Could not get engine input.')
                
        return engine_input_response.json()

    def _init_service(self, engine_input=None, uuid=None):
        self.uu_id = uuid
        print("UUID",self.uu_id)
        if engine_input is None:
            engine_input = self._get_engine_input(self.uu_id)                          
        
        # Get details from table ENTITY TYPE
        self.entity_type = engine_input['resourceName']
        self.entity_type_id = engine_input['resourceId']
        self.entity_type_uu_id = engine_input['resourceUuId']
        self.entity_type_dd_id = engine_input['resourceDdId']
        self.entity_type_type = engine_input['resourceType']
        self.default_db_schema = engine_input['defaultDBSchema']
        self.schema = engine_input['schemaName']
        self.eventTable = engine_input['metricsTableName']
        self.eventTimestampColumn = engine_input['metricTimestampColumn']
        self.entityIdColumn = ENTITY_ID_COLUMN
        self.dimensionTable = engine_input['dimensionsTable']

        self.entityIdName = DATAFRAME_INDEX_ENTITYID
        self.eventTimestampName = DEFAULT_DATAFRAME_INDEX_TIMESTAMP  # use default one if no such data item created

        # Get DATA ITEM details
        self.data_items = DataItemMetadata()
        if 'dataItems' in engine_input and engine_input['dataItems'] is not None:
            for dataItem in engine_input['dataItems']:
                dataItemName = dataItem[DATA_ITEM_NAME_KEY]

                self.data_items.add(dataItem)
                if self.eventTimestampColumn is not None:
                    # Determine the name of the timestamp column in data frame index from definition of timestamp column
                    if (dataItem.get(DATA_ITEM_COLUMN_NAME_KEY, None) is not None and dataItem.get(
                            DATA_ITEM_COLUMN_NAME_KEY, '').upper() == self.eventTimestampColumn.upper()):
                        self.eventTimestampName = dataItemName

        self.logger.info(
            'entity_type_name= {{ %s }} , entity_type_id= {{ %s }}' % (self.entity_type, self.entity_type_id))
        self.logger.info(
            'entity_id_name= {{ %s }}, entity_id_column= {{ %s }}' % (self.entityIdName, self.entityIdColumn))
        self.logger.info('timestamp_name= {{ %s }}, timestamp_column= {{ %s }}' % (
            self.eventTimestampName, self.eventTimestampColumn))
        self.logger.info('dimension_data_items=%s, raw_data_items=%s, derived_data_items=%s' % (
            sorted(self.data_items.get_dimensions()), sorted(self.data_items.get_raw_metrics()),
            sorted(self.data_items.get_derived_metrics())))

        # Get time frequencies
        frequencies = dict()
        if 'frequencies' in engine_input:
            frequencies = engine_input['frequencies']
            frequencies = {v['name']: v['alias'] for v in frequencies}
        self.logger.info('frequencies=%s' % sorted(frequencies))

        # Get granularity sets
        self.granularities = dict()
        if 'granularities' in engine_input and engine_input['granularities'] is not None:
            granularities = engine_input['granularities']
            self.granularities = {v['name']: (
                frequencies.get(v['frequency'], None), tuple(v.get('dimensions', [])), v.get('aggregateByDevice', True),
                v.get('granularitySetId')) for v in granularities}
        self.logger.info('granularities=%s' % sorted(self.granularities.items()))

        # Get KPI declarations
        self._set_pipeline(engine_input.get('kpiDeclarations', []))

        self._entity_constants = engine_input.get('constants')
        if self._entity_constants is None:
            self._entity_constants = []

        self._entity_catalog_functions = engine_input.get('catalogFunctions')
        if self._entity_catalog_functions is None:
            self._entity_catalog_functions = []

        if self.entity_type_type == 'DEVICE_TYPE' and uuid is None:
            self.existing_devices = self.get_devices()
            logger.info(f"Devices of this device type: {self.existing_devices}")
        else:
            # We use entity type name as a kind of pseudo-device id on hierarchy levels
            self.existing_devices = [self.entity_type]
            logger.info(f"Pseudo device name of this resource: {self.existing_devices}")

    def get_kpi_dependency_tree_processing_queue(self, pipeline):
        kpi_tree, sidecar_items, processing_queue = self.parse_kpi_dependency_tree(pipeline=pipeline)
        
        # fill all level 1 into the queue (level 0, raw ones do not need to be inserted)
        leveled_items = defaultdict(list)
        for name, tn in kpi_tree.items():
            # self.logger.debug('last pass data item: ' + str(tn))
            if tn.tree_level() == 1 and tn.name not in sidecar_items:
                processing_queue.append(tn)
            elif tn.tree_level() > 1:
                leveled_items[tn.tree_level()].append(tn)
                
        # now push those with level > 1, in the order of level (2, 3, 4, etc...)
        for level in sorted(leveled_items.keys()):
            for tn in leveled_items[level]:
                if tn.name not in sidecar_items:
                    processing_queue.append(tn)
                    
        if self.logger.isEnabledFor(logging.DEBUG):
            queue_debug_string = 'kpi_dependencies = '
            for q in processing_queue:       
                queue_debug_string += '\n    %s' % q
            self.logger.debug(queue_debug_string)

        return processing_queue

    def parse_kpi_dependency_tree(self, pipeline):
        """Generate the KPI dependency tree hierarchy with raw data items as level 0.

        This method create a treendoe per data item, and fill in the links between nodes
        according to the dependency relationship/hierarchy. Raw data items have level 0,
        derived metrics solely depend on raw items have level 1, so on and so forth. The
        level is determined by the max depth from a node to the root level (raw items).
            
        This method returns a list containing all treenodes, ordered by their level, from
        level 1 upward. The returned list can be used as a queue for processing the data
        items, taking dependency into consideration. It is guranteed that following the
        list sequence (by using list.pop(), that is lowest level at the end of the list)
        any dependency required by a data item would have already been processed.
        """ 
        raw_metrics_set = self.data_items.get_raw_metrics()
        derived_metrics_set = self.data_items.get_derived_metrics()

        processing_queue = list()
            
        kpi_tree = dict() 
                
        # one function can have multiple outputs and it only needs to be invoked once to
        # generate all outputs. since we are creating one treenode per item, we can not
        # put all output items in the returned queue but just one of them, which
        # would avoid invoking the same instance multiple times. here we use a set to
        # store those names not to be put in the final queue
        sidecar_items = set()

        if len(self.pipeline_local) > 0:
            # if not in production mode, figure out any 'virtual' source/target not available
            # from metadata
            raw_metrics_set, derived_metrics_set = self.process_virtual_metrics(self.pipeline_local)

            virtual_raw_metrics_set = raw_metrics_set - self.data_items.get_raw_metrics()
            if len(virtual_raw_metrics_set) > 0:
                raise RuntimeError(
                    'unknown_raw_metrics=%s used in the pipeline, add them as data items to the system first' % str(
                        virtual_raw_metrics_set))

        self.logger.debug(
            'raw_metrics_set=%s, derived_metrics_set=%s' % (str(raw_metrics_set), str(derived_metrics_set)))

        for kpi in pipeline:
            name = get_kpi_targets(kpi)
            sources, scope_sources = self.get_kpi_sources(kpi)

            # Remove 'entity_id' from scope_sources because it is always provided internally and not necessarily
            # defined as raw metric
            scope_sources.discard('entity_id')

            source = sources.union(scope_sources)

            # Check for cyclic dependency between input data items and output data items
            intersection = set(name) & source
            if len(intersection) > 0:
                msg = 'Incorrect cyclic definition of KPI function: The KPI function that is supposed to calculate '
                if len(intersection) > 1:
                    msg += 'the data items %s must not require these data items as input.' % intersection
                else:
                    msg += 'the data item \'%s\' must not require this data item as input.' % intersection.pop()
                raise Exception(msg)

            grain = set()
            if kpi.get(KPI_FUNCTION_GRANULARITY_KEY, None) is not None:
                grain = kpi[KPI_FUNCTION_GRANULARITY_KEY]
                grain = set(grain[1])

            raw_source_nodes = []
            source_nodes = []

            all_sources = (source | grain)
            raw_sources = all_sources & raw_metrics_set
            derived_sources = all_sources & derived_metrics_set
            missing_sources = all_sources - raw_sources - derived_sources

            if len(missing_sources) > 0:
                raise Exception(('The KPI function which calculates data item(s) %s requires ' + (
                    'data item %s as input but this data item has ' if len(
                        missing_sources) == 1 else 'data items %s as input but these data items have ') + 'not been defined neither as raw metric nor as derived metric nor as dimension.') % (
                                    name, list(missing_sources)))

            self.logger.debug('kpi_derived_metrics = %s, kpi_raw_sources=%s, kpi_derived_sources=%s, kpi=%s' % (
                str(name), str(raw_sources), str(derived_sources), str(kpi)))

            # 1st pass can only create nodes for raw sources
            for s in raw_sources:
                tn = KpiTreeNode(name=s, kpi=None, dependency=None, level=0)
                kpi_tree[s] = tn
                raw_source_nodes.append(tn)
                source_nodes.append(tn)

            # for derived ones, simply insert their names first
            source_nodes.extend(derived_sources)

            # for each target, create a treenode with all source nodes as dependency
            for idx, n in enumerate(name):
                kpi_tree[n] = KpiTreeNode(name=n, kpi=kpi, dependency=source_nodes)
                if idx > 0:
                    sidecar_items.add(n)

            if len(name) == 0 and len(source) == 0:
                # it has neither input nor output (data items), put it up front always
                processing_queue.append(
                    KpiTreeNode(name='%s_%s' % (kpi[KPI_FUNCTION_FUNCTIONNAME_KEY], util.randomword(8)), kpi=kpi,
                                dependency=[]))

        # 2nd pass to replace any string source reference with KpiTreeNode source (could only be derived metrics)
        all_aggregators = self.catalog.get_aggregators()
        for k, v in kpi_tree.items():
            if v.dependency is not None:
                for idx, dep in enumerate(v.dependency):
                    if not isinstance(dep, KpiTreeNode):
                        v.dependency[idx] = kpi_tree[dep]
                        kpi_tree[dep].children.add(v)

        for k, v in kpi_tree.items():
            # normally, server side KPI definition takes care of inherited granularity, that is, for
            # transformation or aggregation depending on other kpis, the parent's granularity is
            # checked and the correct granularity is always set for all KPIs
            # but when in client mode, for the 'virtual' kpis, there's no such checking done on the
            # client side, we have to check with another pass for all KPIs without granularity, and
            # for them if any of its parent does have granularity, we set it to be same as its parent
            if v.kpi is not None and v.kpi[KPI_FUNCTION_FUNCTIONNAME_KEY] not in all_aggregators and v.kpi.get(
                    KPI_FUNCTION_GRANULARITY_KEY) is None:
                self.logger.warning('found transformer without granularity: %s' % v.name)

                queue = list()
                queue.extend(v.dependency)
                while len(queue) > 0:
                    dep = queue.pop()
                    if dep.dependency is not None and len(dep.dependency) > 0:
                        for dp in dep.dependency:
                            queue.insert(0, dp)

                    if dep.name in self.target_grains:
                        # set target's grain correctly as well
                        self.target_grains[v.name] = self.target_grains[dep.name]
                        # set it to be the same as its parent
                        v.kpi[KPI_FUNCTION_GRANULARITY_KEY] = self.target_grains[dep.name]
                        # we assume all dependencies must have the same granularity
                        self.logger.debug('granularity=%s' % str(v.kpi[KPI_FUNCTION_GRANULARITY_KEY]))
                        break
                    elif dep.name in self.target_grains_local:
                        # set target's grain correctly as well
                        self.target_grains_local[v.name] = self.target_grains_local[dep.name]
                        # set it to be the same as its parent
                        v.kpi[KPI_FUNCTION_GRANULARITY_KEY] = self.target_grains_local[dep.name]
                        # we assume all dependencies must have the same granularity
                        self.logger.debug('granularity=%s' % str(v.kpi[KPI_FUNCTION_GRANULARITY_KEY]))
                        break

        self.validate_tree(kpi_tree)

        return (kpi_tree, sidecar_items, processing_queue)

    def validate_tree(self, kpi_tree):
        """Validate the tree and remove any invalid node along with all of its descendents."""
        removed = set()
        for name, node in kpi_tree.items():
            kpi = node.kpi
            if kpi is None:
                # raw metric
                continue

            catalog_function = self.catalog.get_function(kpi.get(KPI_FUNCTION_FUNCTIONNAME_KEY))
            if catalog_function is None:
                # remove this node and all its descendents
                removed.add(name)
                removed.update({n.name for n in node.get_all_descendants()})

        for name in removed:
            kpi_tree.pop(name, None)

        return removed

    def get_kpi_sources(self, kpi):
        catalog_function = self.catalog.get_function(kpi.get(KPI_FUNCTION_FUNCTIONNAME_KEY))
        if catalog_function is not None and catalog_function.get(KPI_FUNCTION_INPUT_KEY) is not None:

            sources = get_fn_expression_args(catalog_function, kpi)
                
            # Function 'AggregateWithCalculation' defines an expression which contains the term 'GROUP'. This is incorrectly
            # interpreted as a data item but it is a placeholder for the aggregation groups. Therefore remove it from the list
            # of required data items
            if catalog_function.get(CATALOG_FUNCTION_NAME_KEY) == "AggregateWithCalculation":
                sources.discard("GROUP")

            data_item_input_params = {input.get(FUNCTION_PARAM_NAME_KEY) for input in
                                      catalog_function.get(KPI_FUNCTION_INPUT_KEY, []) if
                                      input.get(FUNCTION_PARAM_TYPE_KEY) == 'DATA_ITEM'}
                    
            for key in data_item_input_params:
                if len(kpi[KPI_FUNCTION_INPUT_KEY]) == 0:
                    logger.warning("Kpi input is empty. Please check the configuration: %s" % (
                        str(kpi)))  # TODO: Check if input is mandatory. If input is mandatory then throw an exception.
                else:       
                    values = kpi[KPI_FUNCTION_INPUT_KEY].get(key)

                    # input of type DATA_ITEM must be a string or a list of string
                    if values is None:
                        # key was not found in input; very likely it is optional and does not show up explicitly
                        continue
                    elif isinstance(values, str):
                        values = [values]
                    elif not isinstance(values, list):
                        logger.warning('data_item_input=(%s: %s) is neither a string nor a list, kpie=%s' % (
                            key, str(values), str(kpi)))
                        continue
                    elif any([not isinstance(src, str) for src in values]):
                        logger.warning(
                            'data_item_input=(%s: %s) is a list but not all its elements are string, kpie=%s' % (
                                key, str(values), str(kpi)))
                        continue

                    sources.update(set(values))

            scope_sources = get_fn_scope_sources(KPI_FUNCTION_SCOPE_KEY, kpi)

        else:
            sources = set()
            scope_sources = set()

        return sources, scope_sources

    def get_all_kpi_sources(self, pipeline):
        all_sources = set()

        for kpi in pipeline:
            sources, scope_sources = self.get_kpi_sources(kpi)
            all_sources.update(sources)
            all_sources.update(scope_sources)

        return all_sources

