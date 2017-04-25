import json
import time
import pymysql
import sys
import logging
import argparse
from datetime import datetime
from datetime import timedelta
import MySQLdb.cursors
import pytz

from influxdb import InfluxDBClient

from synaptiq_json_interface import SynaptiqInterface

# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', help='configuration file')
    arg_parser.add_argument('-l', help='log file')

    args = arg_parser.parse_args()
    config = json.loads(open(args.c).read())

    # --------------------------------------------------------------------------- #
    # Set logging object
    # --------------------------------------------------------------------------- #
    if not args.l:
        log_file = None
    else:
        log_file = args.l
    
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=log_file)
    # --------------------------------------------------------------------------- #
    # Set period and time zone
    # --------------------------------------------------------------------------- #
    if config['time_parameters']['start_date'] == "yesterday":
        start_dt = datetime.utcnow() - timedelta(days=1)
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = start_date
    else:
        start_date = config['time_parameters']['start_date']
        end_date = config['time_parameters']['end_date']
    tz_local = pytz.timezone(config['time_parameters']['time_zone'])

    # --------------------------------------------------------------------------- #
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info("Starting program")

    # --------------------------------------------------------------------------- #
    # MySQL connection
    # --------------------------------------------------------------------------- #

    # Open connection to MySQL server
    logger.info('Connection to MySQL server on socket [%s:%s]' % (config['mysql_connection']['host'],
                                                                  config['mysql_connection']['port']))
    try:
        mysql_conn = pymysql.connect(host=config['mysql_connection']['host'],
                                     port=int(config['mysql_connection']['port']),
                                     user=config['mysql_connection']['user'],
                                     passwd=config['mysql_connection']['password'],
                                     db=config['mysql_connection']['database'],
                                     cursorclass=MySQLdb.cursors.DictCursor)
    except Exception as e:
        logger.error("EXCEPTION: %s" + str(e))
        sys.exit(2)

    logger.info("MySQL connection successful")

    # --------------------------------------------------------------------------- #
    # InfluxDB connection
    # --------------------------------------------------------------------------- #
    logger.info("Connection to InfluxDB server on [%s:%s]" % (config['influxdb_connection']['host'],
                                                              config['influxdb_connection']['port']))
    try:
        idb_client = InfluxDBClient(host=config['influxdb_connection']['host'],
                                    port=int(config['influxdb_connection']['port']),
                                    username=config['influxdb_connection']['user'],
                                    password=config['influxdb_connection']['password'],
                                    database=config['influxdb_connection']['db'])
    except Exception as e:
        logger.error("EXCEPTION: %s" % str(e))
        sys.exit(2)
    logger.info("Connection successful")

    # --------------------------------------------------------------------------- #
    # Get data
    # --------------------------------------------------------------------------- #

    # Login
    si = SynaptiqInterface(url_root=config['3E_synaptiq']['host'], idb_client=idb_client,
                           measurement=config['influxdb_connection']['measurement'],
                           max_lines_per_insert=config['influxdb_connection']['max_lines_per_insert'],
                           tz=tz_local, dst=config['time_parameters']['time_daylight_saving'], logger=logger)
    req_status = si.login(user=config['3E_synaptiq']['user'], password=config['3E_synaptiq']['password'])

    if req_status == 200:
        query = "SELECT P.Id AS P_Id, P.Id_3EObjectId AS P_Id_3EObjectId, P.Name AS P_Name, " + \
                "I.Id AS I_Id, I.Id_3EObjectId AS I_Id_3EObjectId, I.Name AS I_Name " + \
                "FROM 3e_plant AS P, 3e_inverter AS I WHERE P.Id=I.Id_Plant"
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        try:
            with mysql_conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query)
                flag_plant_data = True
                for cur in cursor.fetchall():
                    curr_ts = int(time.mktime(datetime.strptime(start_date, '%Y-%m-%d').timetuple())) * 1e3
                    end_ts = int(time.mktime(datetime.strptime(end_date, '%Y-%m-%d').timetuple())) * 1e3
                    while curr_ts <= end_ts:
                        curr_dt = datetime.fromtimestamp(curr_ts/1e3)
                        curr_date = curr_dt.strftime('%Y-%m-%d')
                        logger.info('Requesting data for day %s, plant=\"%s\", inverter=\"%s\"' % (curr_date,
                                                                                                   cur['P_Name'],
                                                                                                   cur['I_Name']))

                        # Get plant data
                        if flag_plant_data is True:
                            tags = dict(case='PLANT', object_id=cur['P_Id_3EObjectId'], object_name=cur['P_Name'])
                            si.get_data(object_id=int(cur['P_Id_3EObjectId']), ts_from=curr_ts,
                                        ts_to=(curr_ts + (86400 * 1e3)), operator=config['3E_synaptiq']['operator'],
                                        granularity=config['3E_synaptiq']['granularity'], tags=tags)

                        # Get inverter data
                        # STILL TO TEST
                        # tags = dict(case='INVERTER', object_id=cur['I_Id_3EObjectId'], object_name=cur['I_Name'])
                        # si.get_data(object_id=int(cur['I_Id_3EObjectId']), ts_from=curr_ts,
                        #             ts_to=(curr_ts + (86400 * 1e3)), operator=config['3E_synaptiq']['operator'],
                        #             granularity=config['3E_synaptiq']['granularity'], tags=tags)

                        curr_ts += 86400 * 1e3
                        logger.info('Wait a second before exiting or doing a new request')
                        time.sleep(1)
                    flag_plant_data = False
        except Exception as e:
            print('Exception: %s' % str(e))

        # Logout
        si.logout()

        if len(si.influxdb_data_points) > 0:
            logger.info('Sent %i points to InfluxDB server' % len(si.influxdb_data_points))
            idb_client.write_points(si.influxdb_data_points, time_precision='s')
        logger.info('Exit program correctly with code 0')

    else:
        logger.warning('Exit program with code -1')
        sys.exit(-1)



