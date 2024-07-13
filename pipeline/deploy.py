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
    
    kpi_dailyhist_def = {"tenantId":"main","name":"ef279112-c541-4d3a-8b9b-64aa396be59e","description":"\n    Execute a paste-in function. A paste-in function is python function declaration\n    code block. The function must be called 'f' and accept two inputs:\n    df (a pandas DataFrame) and parameters (a dict that you can use\n    to externalize the configuration of the function).\n\n    The function can return a DataFrame,Series,NumpyArray or scalar value.\n\n    Example:\n    def f(df,parameters):\n        #  generate an 2-D array of random numbers\n        output = np.random.normal(1,0.1,len(df.index))\n        return output\n\n    Function source may be pasted in or retrieved from Cloud Object Storage.\n\n    PythonFunction is currently experimental.\n    ","catalogFunctionName":"PythonFunction","enabled":True,"execStatus":False,"granularityName":"Daily","output":{"output_item":"Building-Max_OccupancyCount_Daily_Hist"},"outputMeta":{"Building-Max_OccupancyCount_Daily_Hist":{"dataType":"NUMBER","transient":False}},"input":{"function_code":"def f(df, parameters=None):\n    import numpy as np\n    Input = np.nan_to_num(df['Building-Max_OccupancyCount_Daily'].values)\n    if len(Input) < 10:\n        Output = np.histogram(Input, bins=len(Input))[0] + 0.01\n    else:\n        Output = np.array(np.zeros(len(Input)-10).tolist() + (np.histogram(Input, bins=10)[0]+0.1).tolist())\n    return Output","input_items":["Building-Max_OccupancyCount_Daily"]},"inputMeta":None,"scope":None,"schedule":{"every":"5T","starting_at":"00:00:00"},"backtrack":{"days":20},"granularitySetId":13991,"catalogFunctionVersion":None,"catalogFunctionDto":{"name":"PythonFunction","status":"ACTIVE","description":"\n    Execute a paste-in function. A paste-in function is python function declaration\n    code block. The function must be called 'f' and accept two inputs:\n    df (a pandas DataFrame) and parameters (a dict that you can use\n    to externalize the configuration of the function).\n\n    The function can return a DataFrame,Series,NumpyArray or scalar value.\n\n    Example:\n    def f(df,parameters):\n        #  generate an 2-D array of random numbers\n        output = np.random.normal(1,0.1,len(df.index))\n        return output\n\n    Function source may be pasted in or retrieved from Cloud Object Storage.\n\n    PythonFunction is currently experimental.\n    ","learnMore":None,"moduleAndTargetName":"iotfunctions.bif.PythonFunction","category":"TRANSFORMER","output":[{"name":"output_item","description":"Choose an item name for the function output","learnMore":None,"type":None,"dataType":None,"dataTypeFrom":None,"dataTypeForArray":None,"jsonSchema":None,"cardinalityFrom":None,"tags":[]}],"input":[{"name":"parameters","description":"optional parameters specified in json format","learnMore":None,"type":"CONSTANT","required":False,"dataType":"JSON","dataTypeFrom":None,"dataTypeForArray":None,"values":None,"jsonSchema":None,"defaultValue":None,"tags":[]},{"name":"input_items","description":"Choose one or more data item to use as a function input","learnMore":None,"type":"DATA_ITEM","required":True,"dataType":"ARRAY","dataTypeFrom":None,"dataTypeForArray":None,"values":None,"jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","minItems":1,"items":{"type":"string"}},"defaultValue":None,"tags":[]},{"name":"function_code","description":"Paste in your function definition","learnMore":None,"type":"CONSTANT","required":True,"dataType":"LITERAL","dataTypeFrom":None,"dataTypeForArray":None,"values":None,"jsonSchema":None,"defaultValue":None,"tags":["TEXT"]}],"url":None,"tags":[],"incrementalUpdate":None,"image":None,"version":"V1"}}
      
    kpi_robdaily_def = {"tenantId":"main","name":"fd7d2a3f-c11e-40e8-8505-43ce75903112","description":"\n    Create a new item from an expression involving other items\n    ","catalogFunctionName":"PythonExpression","enabled":True,"execStatus":False,"granularityName":None,"output":{"output_name":"Building_Capacity-RobustDailyMax"},"outputMeta":{"Building_Capacity-RobustDailyMax":{"transient":False}},"input":{"expression":"df['Total_Workpoints_Capacity'] - df['Building-RobustMax_OccupancyCount_Daily']"},"inputMeta":None,"scope":None,"schedule":{},"backtrack":{},"granularitySetId":None,"catalogFunctionVersion":None,"catalogFunctionDto":{"name":"PythonExpression","status":"ACTIVE","description":"\n    Create a new item from an expression involving other items\n    ","learnMore":None,"moduleAndTargetName":"iotfunctions.bif.PythonExpression","category":"TRANSFORMER","output":[{"name":"output_name","description":"Output of expression","learnMore":None,"type":None,"dataType":"NUMBER","dataTypeFrom":None,"dataTypeForArray":None,"jsonSchema":None,"cardinalityFrom":None,"tags":[]}],"input":[{"name":"expression","description":"Define alert expression using pandas syntax. Example: df['inlet_temperature']>50","learnMore":None,"type":"CONSTANT","required":True,"dataType":"LITERAL","dataTypeFrom":None,"dataTypeForArray":None,"values":None,"jsonSchema":None,"defaultValue":None,"tags":["TEXT","EXPRESSION"]}],"url":None,"tags":[],"incrementalUpdate":None,"image":None,"version":"V1"}}    
    
    my_depth = locations[loc_idx]['depth']

    i = loc_idx

    logger.info('Deploy to Building: ' + str(locations[i]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[i]['uuid'] + '/kpiFunctions',
                                         method='post', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)
        
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[i]['uuid'] + '/kpiFunctions',
                                         method='post', json=kpi_dailyhist_def, timeout=60)
        print('Created daily histogram result:', post_res)
        
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[i]['uuid'] + '/kpiFunctions',
                                         method='post', json=kpi_robdaily_def, timeout=60)
        print('Created robust daily max result:', post_res)
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
    
def deploy_dashboard(locations, loc_idx, sitemeta):

    # function definitions to deploy to all Floors
    kpi_dash_def = {"dashboardMetaId":7718,"name":None,"dashboardMetadataJson":{"cards":[{"id":"0d28f728-cbe2-41f4-90b7-fd6a79fceeb7","title":"How many days with n Occupants","size":"MEDIUM","type":"BAR","content":{"type":"SIMPLE","layout":"VERTICAL","series":[{"dataSourceId":"Building-Max_OccupancyCount_Daily_Hist_85f8781a-223d-48cb-87a1-0d58ec612523","label":"Building-Max_OccupancyCount_Daily_Hist","color":"#9f1853"}],"timeDataSourceId":"timestamp"},"dataSource":{"attributes":[{"id":"Building-Max_OccupancyCount_Daily_Hist_85f8781a-223d-48cb-87a1-0d58ec612523","attribute":"Building-Max_OccupancyCount_Daily_Hist"}],"range":{"interval":"day","count":-10,"type":"rolling"}},"timeRange":"last10Days"},{"id":"c6b676ee-451a-4983-ae35-cf0fd9d791e5","title":"Robust Max Occupants per Day","size":"MEDIUM","type":"TIMESERIES","content":{"series":[{"dataSourceId":"Building-RobustMax_OccupancyCount_Daily_2309576d-336d-40b7-adac-766bf489f381","label":"Building-RobustMax_OccupancyCount_Daily","color":"#9f1853"}],"xLabel":"Time","includeZeroOnXaxis":False,"includeZeroOnYaxis":False,"timeDataSourceId":"timestamp","showLegend":True},"dataSource":{"attributes":[{"id":"Building-RobustMax_OccupancyCount_Daily_2309576d-336d-40b7-adac-766bf489f381","attribute":"Building-RobustMax_OccupancyCount_Daily"}],"range":{"interval":"day","count":-7,"type":"rolling"}},"timeRange":"last7Days"},{"id":"deefe5fb-a83c-4553-9472-2c134982175c","title":"Unused seats","size":"SMALL","type":"VALUE","content":{"attributes":[{"dataSourceId":"Building_Capacity-RobustDailyMax_49f6e463-f545-42b4-847e-cb6a8a6c1a3b","label":"Building_Capacity-RobustDailyMax","unit":"   seats","precision":0}]},"dataSource":{"attributes":[{"aggregator":"min","id":"Building_Capacity-RobustDailyMax_49f6e463-f545-42b4-847e-cb6a8a6c1a3b","attribute":"Building_Capacity-RobustDailyMax"}],"range":{"interval":"month","count":-1,"type":"rolling"}},"fontSize":18,"timeRange":"lastMonth"},{"id":"adedacb9-0f76-4fee-8150-8bc4125493eb","title":"Capacity","size":"SMALL","type":"VALUE","content":{"attributes":[{"dataSourceId":"Total_Workpoints_Capacity_2dbf2217-dab8-40a3-a135-9da22438075f","label":"Total_Workpoints_Capacity","unit":"   seats"}]},"dataSource":{"attributes":[{"id":"Total_Workpoints_Capacity_2dbf2217-dab8-40a3-a135-9da22438075f","attribute":"Total_Workpoints_Capacity"}],"range":{"interval":"month","count":-1,"type":"rolling"}},"fontSize":18,"timeRange":"lastMonth"},{"id":"14b1b817-cb26-4354-8b5c-fc9d48205ccb","title":"Strain along rope","size":"MEDIUMTHIN","type":"BAR","content":{"type":"SIMPLE","layout":"VERTICAL","series":[{"dataSourceId":"A-Markus-Test_b669deb0-ff91-457e-badb-85a6eb30486a","label":"A-Markus-Test","color":"#6929c4"}],"timeDataSourceId":"timestamp"},"dataSource":{"attributes":[{"id":"A-Markus-Test_b669deb0-ff91-457e-badb-85a6eb30486a","attribute":"A-Markus-Test"}],"range":{"interval":"day","count":-7,"type":"rolling"}},"timeRange":"last7Days"}],"title":"Space Utilization","layouts":{"lg":[{"w":8,"h":2,"x":0,"y":1,"i":"0d28f728-cbe2-41f4-90b7-fd6a79fceeb7","moved":False,"static":False,"isResizable":True},{"w":8,"h":2,"x":8,"y":0,"i":"c6b676ee-451a-4983-ae35-cf0fd9d791e5","moved":False,"static":False,"isResizable":True},{"w":4,"h":1,"x":0,"y":0,"i":"deefe5fb-a83c-4553-9472-2c134982175c","moved":False,"static":False,"isResizable":True},{"w":4,"h":1,"x":4,"y":0,"i":"adedacb9-0f76-4fee-8150-8bc4125493eb","moved":False,"static":False,"isResizable":True},{"w":4,"h":2,"x":8,"y":2,"i":"14b1b817-cb26-4354-8b5c-fc9d48205ccb","moved":False,"static":False,"isResizable":True}],"md":[{"i":"0d28f728-cbe2-41f4-90b7-fd6a79fceeb7","x":0,"y":0,"w":8,"h":2,"isResizable":True},{"i":"c6b676ee-451a-4983-ae35-cf0fd9d791e5","x":0,"y":4,"w":8,"h":2,"isResizable":True},{"i":"deefe5fb-a83c-4553-9472-2c134982175c","x":0,"y":2,"w":2,"h":1,"isResizable":True},{"i":"adedacb9-0f76-4fee-8150-8bc4125493eb","x":0,"y":2,"w":2,"h":1,"isResizable":True},{"i":"14b1b817-cb26-4354-8b5c-fc9d48205ccb","x":0,"y":6,"w":4,"h":2,"isResizable":True}],"sm":[{"i":"0d28f728-cbe2-41f4-90b7-fd6a79fceeb7","x":0,"y":0,"w":4,"h":2,"isResizable":True},{"i":"c6b676ee-451a-4983-ae35-cf0fd9d791e5","x":0,"y":2,"w":4,"h":2,"isResizable":True},{"i":"deefe5fb-a83c-4553-9472-2c134982175c","x":4,"y":0,"w":2,"h":1,"isResizable":True},{"i":"adedacb9-0f76-4fee-8150-8bc4125493eb","x":4,"y":0,"w":2,"h":1,"isResizable":True},{"i":"14b1b817-cb26-4354-8b5c-fc9d48205ccb","x":4,"y":1,"w":4,"h":2,"isResizable":True}]}},"isEnabled":True,"sequenceNumber":3,"type":"LOCATION","createTimestamp":"2024-06-03T13:38:40.146+00:00","updateTimestamp":"2024-06-25T21:01:50.652+00:00","lastUpdatedBy":"masadmin"}

    my_depth = locations[loc_idx]['depth']

    i = loc_idx

    logger.info('Deploy Dashboard: ' + str(locations[i]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[i]['uuid'] + '/dashboards',
                                             method='post', json=kpi_dash_def, timeout=60)
        print('Created dashboard with cards:', post_res)

    except Exception as e:
        print(e) 
    
def update_deploy_space(locations, loc_idx, sitemeta):
    # function definitions to deploy to all sublocation spaces
    kpi_pe_def = '{"description":"","catalogFunctionName":"PythonExpression","enabled":true,"execStatus":false,"granularityName":null,"output":{"output_name":"OccupSpace_rollavg_15min"},"outputMeta":{"OccupSpace_rollavg_15min":{"transient":false}},"input":{"expression":"sp.signal.savgol_filter(df[\\"Space-OccupancyCount_Minute\\"],15,0)"},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":null,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"PythonExpression","status":"ACTIVE","description":"","learnMore":null,"moduleAndTargetName":"iotfunctions.bif.PythonExpression","category":"TRANSFORMER","output":[{"name":"output_name","description":"Output of expression","learnMore":null,"type":null,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"jsonSchema":null,"cardinalityFrom":null,"tags":[]}],"input":[{"name":"expression","description":"Define alert expression using pandas syntax. Example: df[inlet_temperature]>50","learnMore":null,"type":"CONSTANT","required":true,"dataType":"LITERAL","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":["TEXT","EXPRESSION"]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'
    kpi_agg_def = '{"tenantId":"main","description":"","catalogFunctionName":"AggregateWithExpression","enabled":true,"execStatus":false,"granularityName":"Daily","output":{"name":["Daily_max_15minwin"]},"outputMeta":{"Daily_max_15minwin":{"dataType":"NUMBER","transient":false}},"input":{"expression":"np.round(x.max())","source":["OccupSpace_rollavg_15min"]},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":14393,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"AggregateWithExpression","status":"ACTIVE","description":"","learnMore":null,"moduleAndTargetName":"iotfunctions.bif.AggregateWithExpression","category":"AGGREGATOR","output":[{"name":"name","description":"Choose the data items that you would like to aggregate","learnMore":null,"type":null,"dataType":null,"dataTypeFrom":"source","dataTypeForArray":null,"jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","items":{"type":"string"}},"cardinalityFrom":"source","tags":[]}],"input":[{"name":"source","description":"Choose the data items that you would like to aggregate","learnMore":null,"type":"DATA_ITEM","required":true,"dataType":"ARRAY","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","minItems":"1","items":{"type":"string"}},"defaultValue":null,"tags":[]},{"name":"expression","description":"Paste in or type an AS expression","learnMore":null,"type":"CONSTANT","required":true,"dataType":"LITERAL","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":["TEXT","EXPRESSION"]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'

    my_depth = locations[loc_idx]['depth']

    logger.info('Deploy to Space: ' + str(locations[loc_idx]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='put', json=json.loads(kpi_pe_def), timeout=60)
        print('Created Transformer result:', post_res)
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='put', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)


# locations - list of locations
# loc_idx   - index of the floor to deploy the KPI functions to
# sitemeta  - site metadata for REST call
def update_deploy_floor(locations, loc_idx, sitemeta):

    # function definitions to deploy to all Floors
    kpi_agg_def = '{"tenantId": "main", "name": "fc668a27-f4b1-4002-9073-526bba3800b2", "description": "Calculates the sum of the values in your data set.", "catalogFunctionName": "Sum", "enabled": true, "execStatus": false, "granularityName": "Daily", "output": {"name": "Daily_max_occup_agg"}, "outputMeta": {"Daily_max_occup_agg": {"transient": false}}, "input": {"min_count": 0, "source": "Daily_max_15minwin"}, "inputMeta": null, "scope": null, "schedule": {}, "backtrack": {}, "granularitySetId": 14390, "catalogFunctionVersion": null, "catalogFunctionDto": {"name": "Sum", "status": "ACTIVE", "description": "Calculates the sum of the values in your data set.", "learnMore": null, "moduleAndTargetName": "iotfunctions.aggregate.Sum", "category": "AGGREGATOR", "output": [{"name": "name", "description": "Enter a name for the data item that is produced as a result of this calculation.", "learnMore": null, "type": null, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "jsonSchema": null, "cardinalityFrom": null, "tags": []}], "input": [{"name": "source", "description": "Select the data item that you want to use as input for your calculation.", "learnMore": null, "type": "DATA_ITEM", "required": true, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "values": null, "jsonSchema": null, "defaultValue": null, "tags": []}, {"name": "min_count", "description": "The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.", "learnMore": null, "type": "CONSTANT", "required": false, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "values": null, "jsonSchema": null, "defaultValue": null, "tags": []}], "url": null, "tags": [], "incrementalUpdate": null, "image": null, "version": "V1"}}'

    my_depth = locations[loc_idx]['depth']

    logger.info('Deploy to Floor: ' + str(locations[loc_idx]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='put', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)



# locations - list of locations
# loc_idx   - index of the building to deploy the KPI functions to
# sitemeta  - site metadata for REST call
def update_deploy_building(locations, loc_idx, sitemeta):

    # function definitions to deploy to all Building
    kpi_agg_def = '{"tenantId":"main","name":"26628bbf-356e-4655-a727-2b28417a1e89","description":"Calculates the sum of the values in your data set.","catalogFunctionName":"Sum","enabled":true,"execStatus":false,"granularityName":"Daily","output":{"name":"Daily_occupmax_building_level"},"outputMeta":{"Daily_occupmax_building_level":{"transient":false}},"input":{"min_count":0,"source":"Daily_max_occup_agg"},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":13991,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"Sum","status":"ACTIVE","description":"Calculates the sum of the values in your data set.","learnMore":null,"moduleAndTargetName":"iotfunctions.aggregate.Sum","category":"AGGREGATOR","output":[{"name":"name","description":"Enter a name for the data item that is produced as a result of this calculation.","learnMore":null,"type":null,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"jsonSchema":null,"cardinalityFrom":null,"tags":[]}],"input":[{"name":"source","description":"Select the data item that you want to use as input for your calculation.","learnMore":null,"type":"DATA_ITEM","required":true,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":[]},{"name":"min_count","description":"The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.","learnMore":null,"type":"CONSTANT","required":false,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":[]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'
    
    my_depth = locations[loc_idx]['depth']

    i = loc_idx

    logger.info('Deploy to Building: ' + str(locations[i]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[i]['uuid'] + '/kpiFunctions',
                                         method='put', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)

    logger.info('Proceed to lower levels')

    i += 1
    while i < len(locations) and locations[i]['depth'] > my_depth:

        if locations[i][LOCATION_TYPE] == TRIRIGA_FLOOR:
            update_deploy_floor(locations, i, sitemeta)
        elif locations[i][LOCATION_TYPE] == TRIRIGA_SPACE:
            update_deploy_space(locations, i, sitemeta)
        i += 1

    logger.info('B: Covered ' + str(i - loc_idx) + ' locations')
    
    
    
def delete_deploy_space(locations, loc_idx, sitemeta):
    # function definitions to deploy to all sublocation spaces
    kpi_pe_def = '{"description":"","catalogFunctionName":"PythonExpression","enabled":true,"execStatus":false,"granularityName":null,"output":{"output_name":"OccupSpace_rollavg_15min"},"outputMeta":{"OccupSpace_rollavg_15min":{"transient":false}},"input":{"expression":"sp.signal.savgol_filter(df[\\"Space-OccupancyCount_Minute\\"],15,0)"},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":null,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"PythonExpression","status":"ACTIVE","description":"","learnMore":null,"moduleAndTargetName":"iotfunctions.bif.PythonExpression","category":"TRANSFORMER","output":[{"name":"output_name","description":"Output of expression","learnMore":null,"type":null,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"jsonSchema":null,"cardinalityFrom":null,"tags":[]}],"input":[{"name":"expression","description":"Define alert expression using pandas syntax. Example: df[inlet_temperature]>50","learnMore":null,"type":"CONSTANT","required":true,"dataType":"LITERAL","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":["TEXT","EXPRESSION"]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'
    kpi_agg_def = '{"tenantId":"main","description":"","catalogFunctionName":"AggregateWithExpression","enabled":true,"execStatus":false,"granularityName":"Daily","output":{"name":["Daily_max_15minwin"]},"outputMeta":{"Daily_max_15minwin":{"dataType":"NUMBER","transient":false}},"input":{"expression":"np.round(x.max())","source":["OccupSpace_rollavg_15min"]},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":14393,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"AggregateWithExpression","status":"ACTIVE","description":"","learnMore":null,"moduleAndTargetName":"iotfunctions.bif.AggregateWithExpression","category":"AGGREGATOR","output":[{"name":"name","description":"Choose the data items that you would like to aggregate","learnMore":null,"type":null,"dataType":null,"dataTypeFrom":"source","dataTypeForArray":null,"jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","items":{"type":"string"}},"cardinalityFrom":"source","tags":[]}],"input":[{"name":"source","description":"Choose the data items that you would like to aggregate","learnMore":null,"type":"DATA_ITEM","required":true,"dataType":"ARRAY","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":{"$schema":"http://json-schema.org/draft-07/schema#","type":"array","minItems":"1","items":{"type":"string"}},"defaultValue":null,"tags":[]},{"name":"expression","description":"Paste in or type an AS expression","learnMore":null,"type":"CONSTANT","required":true,"dataType":"LITERAL","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":["TEXT","EXPRESSION"]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'

    my_depth = locations[loc_idx]['depth']

    logger.info('Deploy to Space: ' + str(locations[loc_idx]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='delete', json=json.loads(kpi_pe_def), timeout=60)
        print('Created Transformer result:', post_res)
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='delete', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)


# locations - list of locations
# loc_idx   - index of the floor to deploy the KPI functions to
# sitemeta  - site metadata for REST call
def delete_deploy_floor(locations, loc_idx, sitemeta):

    # function definitions to deploy to all Floors
    kpi_agg_def = '{"tenantId": "main", "name": "fc668a27-f4b1-4002-9073-526bba3800b2", "description": "Calculates the sum of the values in your data set.", "catalogFunctionName": "Sum", "enabled": true, "execStatus": false, "granularityName": "Daily", "output": {"name": "Daily_max_occup_agg"}, "outputMeta": {"Daily_max_occup_agg": {"transient": false}}, "input": {"min_count": 0, "source": "Daily_max_15minwin"}, "inputMeta": null, "scope": null, "schedule": {}, "backtrack": {}, "granularitySetId": 14390, "catalogFunctionVersion": null, "catalogFunctionDto": {"name": "Sum", "status": "ACTIVE", "description": "Calculates the sum of the values in your data set.", "learnMore": null, "moduleAndTargetName": "iotfunctions.aggregate.Sum", "category": "AGGREGATOR", "output": [{"name": "name", "description": "Enter a name for the data item that is produced as a result of this calculation.", "learnMore": null, "type": null, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "jsonSchema": null, "cardinalityFrom": null, "tags": []}], "input": [{"name": "source", "description": "Select the data item that you want to use as input for your calculation.", "learnMore": null, "type": "DATA_ITEM", "required": true, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "values": null, "jsonSchema": null, "defaultValue": null, "tags": []}, {"name": "min_count", "description": "The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.", "learnMore": null, "type": "CONSTANT", "required": false, "dataType": "NUMBER", "dataTypeFrom": null, "dataTypeForArray": null, "values": null, "jsonSchema": null, "defaultValue": null, "tags": []}], "url": null, "tags": [], "incrementalUpdate": null, "image": null, "version": "V1"}}'

    my_depth = locations[loc_idx]['depth']

    logger.info('Deploy to Floor: ' + str(locations[loc_idx]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[loc_idx]['uuid'] + '/kpiFunctions',
                                             method='delete', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)



# locations - list of locations
# loc_idx   - index of the building to deploy the KPI functions to
# sitemeta  - site metadata for REST call
def delete_deploy_building(locations, loc_idx, sitemeta):

    # function definitions to deploy to all Building
    kpi_agg_def = '{"tenantId":"main","name":"26628bbf-356e-4655-a727-2b28417a1e89","description":"Calculates the sum of the values in your data set.","catalogFunctionName":"Sum","enabled":true,"execStatus":false,"granularityName":"Daily","output":{"name":"Daily_occupmax_building_level"},"outputMeta":{"Daily_occupmax_building_level":{"transient":false}},"input":{"min_count":0,"source":"Daily_max_occup_agg"},"inputMeta":null,"scope":null,"schedule":{},"backtrack":{},"granularitySetId":13991,"catalogFunctionVersion":null,"catalogFunctionDto":{"name":"Sum","status":"ACTIVE","description":"Calculates the sum of the values in your data set.","learnMore":null,"moduleAndTargetName":"iotfunctions.aggregate.Sum","category":"AGGREGATOR","output":[{"name":"name","description":"Enter a name for the data item that is produced as a result of this calculation.","learnMore":null,"type":null,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"jsonSchema":null,"cardinalityFrom":null,"tags":[]}],"input":[{"name":"source","description":"Select the data item that you want to use as input for your calculation.","learnMore":null,"type":"DATA_ITEM","required":true,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":[]},{"name":"min_count","description":"The required number of valid values to perform the operation. If fewer than min_count non-NA values are present the result will be NA. Defalut 1.","learnMore":null,"type":"CONSTANT","required":false,"dataType":"NUMBER","dataTypeFrom":null,"dataTypeForArray":null,"values":null,"jsonSchema":null,"defaultValue":null,"tags":[]}],"url":null,"tags":[],"incrementalUpdate":null,"image":null,"version":"V1"}}'

    my_depth = locations[loc_idx]['depth']

    i = loc_idx

    logger.info('Deploy to Building: ' + str(locations[i]['alias']))

    try:
        post_res = util.api_request('/api/v2/core/sites/' + sitemeta[0]['uuid'] + '/locations/' + locations[i]['uuid'] + '/kpiFunctions',
                                         method='delete', json=json.loads(kpi_agg_def), timeout=60)
        print('Created Aggregator result:', post_res)

    except Exception as e:
        print(e)

    logger.info('Proceed to lower levels')

    i += 1
    while i < len(locations) and locations[i]['depth'] > my_depth:

        if locations[i][LOCATION_TYPE] == TRIRIGA_FLOOR:
            delete_deploy_floor(locations, i, sitemeta)
        elif locations[i][LOCATION_TYPE] == TRIRIGA_SPACE:
            delete_deploy_space(locations, i, sitemeta)
        i += 1

    logger.info('B: Covered ' + str(i - loc_idx) + ' locations')

