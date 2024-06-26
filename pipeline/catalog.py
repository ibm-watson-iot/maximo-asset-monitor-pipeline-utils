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
# This catalog class keeps track of functions registered with a pipeline
# It is part of the pipeline renderer to check whether custom functions are available

import importlib
import logging
import subprocess
from collections import defaultdict

from iotfunctions.exceptions import ApplicationException
from iotfunctions.loader import BaseLoader

CATEGORY_TRANSFORMER = 'TRANSFORMER'
CATEGORY_AGGREGATOR = 'AGGREGATOR'
CATEGORY_ALERT = 'ALERT'
CATALOG_REGISTER_TEMPLATE = '/api/catalog/v1/{0}/function/{1}'

logger = logging.getLogger(__name__)

class Catalog(object):
    def __init__(self):
        self.catalogs = defaultdict(dict)

    def get_function(self, name):
        return self.catalogs.get(name, None)

    def transformers(self, include_alerts=True):
        if include_alerts:
            return {k: v for k, v in self.catalogs.items() if
                    v['category'] == CATEGORY_TRANSFORMER or v['category'] == CATEGORY_ALERT}
        return {k: v for k, v in self.catalogs.items() if v['category'] == CATEGORY_TRANSFORMER}

    def aggregators(self):
        return {k: v for k, v in self.catalogs.items() if v['category'] == CATEGORY_AGGREGATOR}

    def alerts(self):
        return {catalog_function_name: catalog_function for catalog_function_name, catalog_function in
                self.catalogs.items() if catalog_function['category'] == CATEGORY_ALERT}

    def _get_class(self, kls):
        parts = kls.split('.')
        modulePath = '.'.join(parts[:-1])
        className = parts[-1]
        # shortcut for analytics_services function
        #if className == 'ChildDataLoader': return ''
        print(parts)
        if parts[0] == 'analytics_service': return ''
        importlib.invalidate_caches()
        modul = None
        try:
            modul = importlib.import_module(modulePath)
        except Exception as cle:
            print(cle)
            return None
        return getattr(modul, className)

    def _get_function(self, function):
        if '.' in function['module_and_target_name']:
            target = function['module_and_target_name']
        elif function['module_and_target_name'] in ['sum', 'min', 'max', 'mean', 'count', 'median', 'std', 'var',
                                                    'prod', 'first', 'last']:
            # pandas optimized agg methods, even though we don't use some of them, they can still be used by users
            return function['module_and_target_name']
        else:
            target = 'builtins.' + function['module_and_target_name']

        try:
            return self._get_class(target)
        except Exception:
            logger.warning('error importing target %s' % target, exc_info=True)
            return None

    def _get_functions(self, function_dict):
        functions = {}
        for k, v in function_dict.items():
            function = self._get_function(v)
            if function is not None:
                functions[k] = function
        return functions

    def get_loaders(self):
        return {fname: func for fname, func in self.get_transformers().items() if issubclass(func, BaseLoader)}

    def get_alerts(self):
        return self._get_functions(self.alerts())

    def get_transformers(self):
        return self._get_functions(self.transformers())

    def get_aggregators(self):
        return self._get_functions(self.aggregators())

    def register_transformer(self, name, module_and_target_name, output_params, description=None, url=None,
                             input_params=[], tags=[], incremental_update=None, tenant_id=None):
        if name is None:
            raise ValueError('missing mandatory parameter name')
        if module_and_target_name is None:
            raise ValueError('missing mandatory parameter module_and_target_name')
        if output_params is None:
            raise ValueError('missing mandatory parameter output_params')
        if tenant_id is not None:
            if description is None:
                raise ValueError('missing mandatory parameter description')
            if url is None:
                raise ValueError('missing mandatory parameter url')

        if not self._register_local(name=name, category=CATEGORY_TRANSFORMER,
                                    module_and_target_name=module_and_target_name, url=url, input_params=input_params,
                                    output_params=output_params, incremental_update=incremental_update):
            return False

        if tenant_id is not None:
            if not self._register_remote(tenant_id=tenant_id, name=name, description=description,
                                         category=CATEGORY_TRANSFORMER, tags=tags,
                                         module_and_target_name=module_and_target_name, url=url,
                                         input_params=input_params, output_params=output_params,
                                         incremental_update=None):
                self._unregister_local(name)
                return False

        return True

    def register_aggregator(self, name, module_and_target_name, output_params, description=None, url=None,
                            input_params=[], tags=[], incremental_update=None, tenant_id=None):
        if name is None:
            raise ValueError('missing mandatory parameter name')
        if module_and_target_name is None:
            raise ValueError('missing mandatory parameter module_and_target_name')
        if output_params is None:
            raise ValueError('missing mandatory parameter output_params')
        if tenant_id is not None:
            if description is None:
                raise ValueError('missing mandatory parameter description')
            if url is None:
                raise ValueError('missing mandatory parameter url')

        if not self._register_local(name=name, category=CATEGORY_AGGREGATOR,
                                    module_and_target_name=module_and_target_name, url=url, input_params=input_params,
                                    output_params=output_params, incremental_update=incremental_update):
            return False

        if tenant_id is not None:
            if not self._register_remote(tenant_id=tenant_id, name=name, description=description,
                                         category=CATEGORY_AGGREGATOR, tags=tags,
                                         module_and_target_name=module_and_target_name, url=url,
                                         input_params=input_params, output_params=output_params,
                                         incremental_update=incremental_update):
                self._unregister_local(name)
                return False

        return True

    def unregister(self, name, tenant_id=None):
        if tenant_id is not None:
            self._unregister_remote(tenant_id, name)

        return self._unregister_local(name)

    def load_custom_functions(self, import_local=True, catalog_functions=[]):
        url_installed = set()

        for func in catalog_functions:
            # if func['name'] in ['Sum', 'Minimum', 'Maximum', 'Mean', 'Median', 'Count', 'DistinctCount', 'StandardDeviation', 'Variance', 'Product', 'First', 'Last']:
            #     func['moduleAndTargetName'] = 'analytics_service.aggregate.' + func['name']
            # elif func.get('packageName') is not None and len(func['packageName']) > 0:
            if func.get('packageName') is not None and len(func['packageName']) > 0:
                func['moduleAndTargetName'] = func['packageName'] + '.' + func['moduleAndTargetName']

            install = False
            if func['url'] is not None and func['url'] not in url_installed:
                install = True
                url_installed.add(func['url'])

            function_registration_successful = self._register_local(name=func['name'],
                                                                    category=func['category'].upper(),
                                                                    module_and_target_name=func['moduleAndTargetName'],
                                                                    url=func['url'], input_params=func['input'],
                                                                    output_params=func['output'],
                                                                    incremental_update=func.get('incrementalUpdate',
                                                                                                None),
                                                                    import_local=import_local, install=install)
            if not function_registration_successful:
                error_message = "Unable to load the custom function: %s" % func['moduleAndTargetName']
                raise ApplicationException(error_message)

        logger.info('transformers=%s' % sorted([func for func in self.transformers()]))
        logger.info('alerts=%s' % sorted([func for func in self.get_alerts()]))
        logger.info('aggregators=%s' % sorted(
            ['%s:%s' % (func, func_meta.get('incremental_update', 'none')) for func, func_meta in
             self.aggregators().items()]))

        return True

    def _register_remote(self, tenant_id, name, description, category, tags, module_and_target_name, url, input_params,
                         output_params, incremental_update, raise_error=True, ):
        if tenant_id is None or name is None or category is None or module_and_target_name is None:
            return False

        payload = {'name': name, 'description': description, 'category': category, 'tags': tags,
                   'moduleAndTargetName': module_and_target_name, 'url': url, 'input': input_params,
                   'output': output_params,
                   'incremental_update': incremental_update if category == CATEGORY_AGGREGATOR else None, }

        # resp = api_request(CATALOG_REGISTER_TEMPLATE.format(tenant_id, name), method='put', json=payload)
        resp = self.db.http_request(object_type='function', object_name=name, request="PUT", payload=payload,
                                    raise_error=raise_error)
        return (resp is not None)

    def _unregister_remote(self, tenant_id, name):
        # resp = api_request(CATALOG_REGISTER_TEMPLATE.format(tenant_id, name), method='delete')
        resp = self.db.http_request(object_type='function', object_name=name, request="DELETE", raise_error=False)
        return (resp is not None)

    def _install(self, url):

        try:
            """
            Install python package located at URL
            """
            # adding host_url as trusted host for internal MAS url. Issue #1172
            split_attributes = url.split("://")
            i = (0, 1)[len(split_attributes) > 1]
            host_url = split_attributes[i].split("?")[0].split('/')[0].split(':')[0]

            msg = 'running pip install'
            logger.debug(msg)

            #if host_url.lower().endswith(".svc"):
            #    sequence = ['pip', 'install', '--no-cache-dir', '--trusted-host', host_url, '--upgrade', url]
            #else:
            sequence = ['pip', 'install', '--break-system-packages', '--no-cache-dir', '--trusted-host', host_url, '--upgrade', url]

            completed_process = subprocess.run(sequence, stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                                               universal_newlines=True)

            if completed_process.returncode == 0:
                logger.info('pip install was successful: \n %s' % completed_process.stdout)
                return True
            else:
                logger.error('pip install failed: \n %s' % completed_process.stdout)
                return False

        except:
            logger.error('error while calling pip to install a custom function', exc_info=True)
            return False

    def _register_local(self, name, category, module_and_target_name, url, input_params, output_params,
                        incremental_update, import_local=True, install=True):
        if install and url is not None:
            if not self._install(url):
                return False

        function = {'name': name, 'category': category, 'module_and_target_name': module_and_target_name, 'url': url,
                    'input': input_params, 'output': output_params,
                    'incremental_update': incremental_update if category == CATEGORY_AGGREGATOR else None, }

        #print(function)

        if not import_local or self._get_function(function) is not None:
            self.catalogs[name] = function
            return True
        else:
            logger.warning('error loading function %s' % name)
            return False

    def _unregister_local(self, name):
        if name in self.catalogs:
            del self.catalogs[name]
            return True
        else:
            return False
