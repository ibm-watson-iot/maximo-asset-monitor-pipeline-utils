# *****************************************************************************
# © Copyright IBM Corp. 2018, 2024  All Rights Reserved.
#
# This program and the accompanying materials
# are made available under the terms of the Apache V2.0 license
# which accompanies this distribution, and is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# *****************************************************************************

import logging.config
import os
import random
import re
import string

import pandas as pd
import pandas.tseries.frequencies
import requests

from iotfunctions import util

SSL_VERIFY = False
API_BASEURL = os.environ.get('API_BASEURL', None)
API_KEY = os.environ.get('API_KEY', None)
API_TOKEN = os.environ.get('API_TOKEN', None)
API_BASICAUTH = os.environ.get('API_BASICAUTH', None)
TENANT_ID = os.environ.get('TENANT_ID', None)

# Dimension column name for time zone
DIM_TIME_ZONE = "TimeZone"

logger = logging.getLogger(__name__)


def setup_credentials(api_baseurl=None, api_key=None, api_token=None, tenant_id=None):
    global API_BASEURL, API_KEY, API_TOKEN, TENANT_ID
    API_BASEURL = api_baseurl
    API_KEY = api_key
    API_TOKEN = api_token
    TENANT_ID = tenant_id

def api_request(path, method='get', json=None, timeout=30):
    url = API_BASEURL + path
    logger.info('api_request_url=%s' % url)

    if API_KEY is not None and API_TOKEN is not None:
        headers = {'X-api-key': API_KEY, 'X-api-token': API_TOKEN, 'Content-type': 'application/json'}
    elif API_BASICAUTH is not None:
        headers = {'Authorization': 'Basic %s' % API_BASICAUTH, 'Content-type': 'application/json'}
    else:
        raise RuntimeError('cannot find API credentials from the environment variables')

    headers["tenantId"] = TENANT_ID
    headers["mam_user_email"] = "pipeline@ibm.com"

    print(headers)

    if method == 'get':
        resp = requests.get(url, headers=headers, verify=SSL_VERIFY, timeout=timeout)
    elif method == 'post':
        resp = requests.post(url, headers=headers, verify=SSL_VERIFY, json=json, timeout=timeout)
    elif method == 'put':
        resp = requests.put(url, headers=headers, verify=SSL_VERIFY, json=json, timeout=timeout)
    elif method == 'delete':
        resp = requests.delete(url, headers=headers, verify=SSL_VERIFY, timeout=timeout)
    else:
        raise RuntimeError('unsupported_method=%s' % method)

    if resp.status_code != requests.codes.ok:
        logger.warning('error api request: url=%s, method=%s, status_code=%s, response_text=%s' % (
            url, method, str(resp.status_code), str(resp.text)))
        return None

    return resp


def asList(x):
    if not isinstance(x, list):
        x = [x]
    return x


def next_schedule(interval_start, interval, now):
    """
    Determine the next schedule after now

    :param interval_start: pandas's Timestamp or string holding time or date + time
    :param interval: Frequency string or pandas's DateOffset
    :param now: pandas's Timestamp
    :return: pandas's Timestamp
    """

    if isinstance(interval, str):
        # Convert frequency string to pandas's DateOffset
        try:
            interval = pd.tseries.frequencies.to_offset(interval)
        except Exception as ex:
            raise Exception("The conversion of frequency string '%s' to pandas's DataOffset object failed.") from ex

    elif not isinstance(interval, pd.DateOffset):
        raise Exception(
            "The variable interval is neither a frequency string nor a pandas's DataOffset object: %s" % interval.__class__)

    if isinstance(interval_start, str):
        # Convert interval_start from string to pd.Timestamp
        if re.match('\d{4}\D?\d{2}\D?\d{2}\D?\d{1,2}\D?\d{2}(\D?\d{2}(\.\d{1,6})?)?', interval_start) is not None:
            # interval_start holds date + time
            try:
                interval_start = pd.Timestamp(interval_start)
            except Exception as ex:
                raise Exception(
                    "Conversion of date-time string '%s' to a pandas's Timestamp object failed." % interval_start) from ex

        elif re.match('\d{1,2}\D?\d{2}(\D?\d{2}(\.\d{1,6})?)?', interval_start) is not None:
            # interval_start only holds time but no date. Take January, 1st of current year as date
            try:
                date = '%s-%s-%s ' % (now.year, 1, 1)
                interval_start = pd.Timestamp(date + interval_start)
            except Exception as ex:
                raise Exception(
                    "Conversion of time string '%s' to a pandas's Timestamp object failed." % interval_start) from ex

        else:
            raise Exception("String '%s' cannot be converted to a pandas's Timestamp object." % interval_start)

    elif not isinstance(interval_start, pd.Timestamp):
        raise Exception(
            "Variable interval_start is neither a string not a pandas's Timestamp object: %s" % interval_start.__class__)

    if isinstance(interval, (pd.offsets.Hour, pd.offsets.Minute, pd.offsets.Second, pd.offsets.Milli, pd.offsets.Micro,
                             pd.offsets.Nano)):
        # These DateOffsets can be handled like time differences because they define a fixed shift in time.
        # Find a good starting point for the subsequent while loop for performance. Actually, next_schedule is already
        # the searched point in time or the point in time right before the search one.
        next_schedule = interval_start + ((now - interval_start) // interval) * interval
    else:
        # The shift in time of all other DateOffsets are not fixed, for example the shift of pd.offset.Day can be
        # 23 hour in case daylight saving time period starts, the shift of MonthBegin might be 28, 29, 30, or 31 days
        # Shift backwards interval_start to a point in time that matches the condition of the corresponding DateOffset.
        next_schedule = interval.rollback(interval_start)

    # Move schedule forward until we find the first schedule after now
    while next_schedule < now:
        next_schedule += interval

    logger.debug(
        'now=%s, interval_start=%s, interval=%s, next_schedule=%s' % (now, interval_start, interval, next_schedule))

    return next_schedule


def is_schedule_up(batch_start, batch_end, interval_start, interval):
    """Check if the next schedule falls within the given time period.

    Arguments:
    batch_start, batch_end --   two pd.Timestamp objects, within which to check
                                if the schedule should be up

    Keyword arguments:
    interval_start --   the base time for schedule, can be either pd.Timestamp object
                        or a string like "2020-09-24 12:34:56" or "12:34:56".
    interval -- the periodical scheduling interval, can be either 
                pd.DateOffset or a string which can be parsed by pd.tseries.frequencies.to_offset
    """

    if batch_start is None:
        # No batch_start defined for this pipeline run (actually, it's minus infinity). This is the first run
        # of pipeline. Therefore all KPI functions are due.
        schedule_is_up = True

    else:
        next_scheduled = next_schedule(interval_start=interval_start, interval=interval, now=batch_start)
        schedule_is_up = (batch_start <= next_scheduled < batch_end)

    return schedule_is_up


def randomword(length):
    letters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(letters) for i in range(length))


def log_data_frame(message=None, df=None):
    return util.log_data_frame(message, df)


class TimeZones():

    OFFSET_TIMEZONE_UTC = pd.Timedelta(0)

    def __init__(self, deviceid_to_timezone):

        self.device_to_offset = {}
        offset_to_devices = {}
        for deviceid, offset in deviceid_to_timezone.items():
            if pd.isna(offset):
                offset = self.OFFSET_TIMEZONE_UTC
            else:
                tmp_result = re.findall('([+-]?)\s*(\d{1,2}):?(\d{2})?', offset)
                if len(tmp_result) > 0:
                    hours = int(tmp_result[0][0] + tmp_result[0][1])
                    if len(tmp_result[0][2]) > 0:
                        minutes = int(tmp_result[0][2])
                    else:
                        minutes = 0
                    offset = pd.Timedelta(hours=hours, minutes=minutes)
                else:
                    raise RuntimeError(f'The dimension table contains an invalid string for device {deviceid} in column {DIM_TIME_ZONE}. '
                                       f'No offset of format [+-]XX[:XX] was found in the following string: {offset}')
            self.device_to_offset[deviceid] = offset
            device_set = offset_to_devices.get(offset, set())
            device_set.add(deviceid)
            offset_to_devices[offset] = device_set

        self.offset_to_devices = {}
        self.minimum_offset = self.OFFSET_TIMEZONE_UTC
        self.maximum_offset = self.OFFSET_TIMEZONE_UTC
        for offset, device_set in offset_to_devices.items():
            self.offset_to_devices[offset] = sorted(device_set)
            if offset < self.minimum_offset:
                self.minimum_offset = offset
            if offset > self.maximum_offset:
                self.maximum_offset = offset

        logger.info(f'Device to offset mapping: {sorted(self.device_to_offset.items(), key=lambda x: x[0])}')
        logger.info(f'Offset to device mapping: {sorted(self.offset_to_devices.items(), key=lambda x: x[0])}')
        logger.info(f'Minimum offset={self.minimum_offset}, maximum offset={self.maximum_offset}')

    def get_for_device(self, device_id):
        return self.device_to_offset.get(device_id, pd.Timedelta(0))

    def get_devices(self, offset):
        return self.offset_to_devices.get(offset, {})

    def get_offsets(self):
        return self.offset_to_devices.keys()

    def items(self):
        return self.offset_to_devices.items()

    def get_minimum_offset(self):
        return self.minimum_offset

    def get_maximum_offset(self):
        return self.maximum_offset


def copy_timezone_enabled_df(df_tz):
    if df_tz is not None:
        copied_df_tz = {}
        for offset, tmp_df in df_tz.items():
            if tmp_df is not None:
                copied_df_tz[offset] = tmp_df.copy()
            else:
                copied_df_tz[offset] = None
    else:
        copied_df_tz = None

    return copied_df_tz
