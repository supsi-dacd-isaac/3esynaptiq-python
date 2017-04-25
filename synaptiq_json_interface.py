import requests
import json
import time
import datetime
import logging
import calendar
import pytz
import sys

class SynaptiqInterface:
    """
    Class for 3E Synaptiq interface
    """

    def __init__(self, url_root, idb_client, max_lines_per_insert, measurement, tz, dst, logger):
        """ 
        Constructor
        @:param url_root: url of 3E Synaptiq server
        @:type url_root: string
        @:param idb_client: InfluxDB client object
        @:type idb_client: InfluxDBClient
        @:param measurement: InfluxDb measurement name
        @:type measurement: string         
        @:param max_lines_per_insert: Maximum lines available in an InfluxDb query
        @:type max_lines_per_insert: int        
        @:param tz: time zone object
        @:type tz: pytz
        @:param dst: daylight saving time flag
        @:type dst: string                        
        @:param logger: logger
        @:type logger: logging object        
        """
        self.url_root = url_root
        self.url_login = '%s/application/login' % self.url_root
        self.url_logout = '%s/application/logout' % self.url_root
        self.url_parklist = '%s/park/list' % self.url_root
        self.url_grouplist = '%s/group/list' % self.url_root
        self.url_domainlist = '%s/application/domain' % self.url_root
        self.url_parkdetail = '%s/park/detail' % self.url_root
        self.url_getdata = '%s/application/data' % self.url_root

        self.idb_client = idb_client
        self.max_lines_per_insert = max_lines_per_insert
        self.measurement = measurement
        self.tz = tz
        self.logger = logger
        self.dst = dst

        self.session_id = None
        self.user = None
        self.password = None
        self.influxdb_data_points = []

    def login(self, user, password):
        """ 
        Login method
        @:param user: 3E user
        @:type user: string
        @:param password: 3E password
        @:type password: string
        @:return: HTTP request status
        @:rtype: int        
        """
        self.user = user
        self.password = password
        params = dict(userDetails=dict(username=self.user, password=self.password))
        r = requests.post(self.url_login, data=json.dumps(params))
        self.logger.info('Login request to %s -> response status %i' % (self.url_login, r.status_code))
        if r.status_code == 200:
            data = json.loads(r.text)
            self.session_id = data['sessionId']
            self.logger.info('Login successful, sessionId=%s' % self.session_id)
        else:
            self.logger.warning('Login failed')
        return r.status_code

    def logout(self):
        """ 
        Logout method
        """
        if self.session_id is not None:
            data = dict(sessionId=self.session_id)
            r = requests.post(self.url_logout, data=json.dumps(data))
            self.logger.info('Logout request to %s -> response status %i' % (self.url_logout, r.status_code))
            if r.status_code == 200:
                self.logger.info('Logout successful')
                self.session_id = None
        else:
            self.logger.warning('Unable to logout')

    def get_data(self, object_id, ts_from, ts_to, operator, granularity, tags):
        params = {
                    'sessionId': self.session_id,
                    'objectId': object_id,  # plant
                    'requests': [
                                    {
                                        'indicator': operator,
                                        'granularity': granularity,
                                        'from': ts_from,
                                        'to': ts_to,
                                        'aggregated': 'false'
                                    }
                                ]
                }

        r_data = requests.post(self.url_getdata, data=json.dumps(params))
        data = json.loads(r_data.text)

        for j in range(0, len(data['data'])):
            str_desc_signal = '\"objectId=%s;indicator=%s\"' % (data['data'][j]['objectId'],
                                                                data['data'][j]['indicator'])
            if 'samples' in data['data'][j].keys():
                self.logger.info('%s -> found %i samples' % (str_desc_signal, len(data['data'][j]['samples'])))

                for k in range(0, len(data['data'][j]['samples'])):
                    # Time management
                    naive_time = datetime.datetime.fromtimestamp(data['data'][j]['samples'][k]['timestamp'] / 1e3)
                    if self.dst == 'True':
                        local_dt = self.tz.localize(naive_time, is_dst=True)
                    else:
                        local_dt = self.tz.localize(naive_time)
                    utc_dt = local_dt.astimezone(pytz.utc)

                    if 'min' in granularity:
                        # Transform data from kWh to kW
                        granularity_num = float(granularity[:-3])
                        value = data['data'][j]['samples'][k]['value'] * 60 / granularity_num
                    else:
                        # For granularity >= 1h values are maintained in kWh
                        value = data['data'][j]['samples'][k]['value']

                    # Build point section
                    point = {
                                'time': int(calendar.timegm(datetime.datetime.timetuple(utc_dt))),
                                'measurement': self.measurement,
                                'fields': dict(value=value),
                                'tags': tags
                            }
                    self.influxdb_data_points.append(point)

                    if len(self.influxdb_data_points) >= int(self.max_lines_per_insert):
                        self.logger.info('Sent %i points to InfluxDB server' % len(self.influxdb_data_points))
                        self.idb_client.write_points(self.influxdb_data_points, time_precision='s')
                        self.influxdb_data_points = []
            else:
                self.logger.warning('%s -> found 0 samples' % str_desc_signal)

            self.logger.info('Wait for 1 second')
            time.sleep(1)
