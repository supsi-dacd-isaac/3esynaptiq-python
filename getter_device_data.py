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
# Functions
# --------------------------------------------------------------------------- #

# --------------------------------------------- ------------------------------ #
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
    # Starting program
    # --------------------------------------------------------------------------- #
    logger.info("Starting program")

    # --------------------------------------------------------------------------- #
    # Set period and time zone
    # --------------------------------------------------------------------------- #
    time_frmt = '%Y-%m-%d %H'
    if int(config['time_parameters']['last_hours']) > 24:
        last_hours = 24
        logger.warning("last_hours parameter set to %i (maximum)" % last_hours)
    else:
        last_hours = int(config['time_parameters']['last_hours'])
    start_dt = datetime.utcnow() - timedelta(hours=last_hours)
    start_date = start_dt.strftime(time_frmt)
    start_ts = int(time.mktime(datetime.strptime(start_date, time_frmt).timetuple())) * 1e3
    tz_local = pytz.timezone(config['time_parameters']['time_zone'])

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
        # Get inverter metadata from MySQL database
        query = "SELECT P.Id AS P_Id, P.Id_3EObjectId AS P_Id_3EObjectId, P.Name AS P_Name, I.Id AS I_Id, " + \
                "I.Id_3EObjectId AS I_Id_3EObjectId, I.Name AS I_Name, I.Description AS I_Description " + \
                "FROM 3e_plant AS P, 3e_inverter AS I WHERE P.Id=I.Id_Plant AND P.Id=" + \
                str(config['mysql_connection']['id_plant'])
        id_plant = -1
        inverters_data = dict()
        cursor = mysql_conn.cursor(pymysql.cursors.DictCursor)
        try:
            with mysql_conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query)
                flag_plant_data = True
                for cur in cursor.fetchall():
                    single_inverter_data = dict(case='INVERTER', object_id=cur['I_Id_3EObjectId'],
                                                object_name=cur['I_Name'], park_id=cur['P_Id_3EObjectId'],
                                                park_name=cur['P_Name'])
                    id_plant = int(cur['P_Id_3EObjectId'])
                    inverters_data[cur['I_Id_3EObjectId']] = single_inverter_data
        except Exception as e:
            print('Exception: %s' % str(e))

        # Request inverter data to 3E web service
        logger.info('Requesting last %i hours of data, level="INVERTER";signal=\"%s\"' %
                    (last_hours, config['3E_synaptiq']['indicator']))
        req_status = si.get_data_inverter(signal=config['3E_synaptiq']['indicator'],
                                          granularity=config['3E_synaptiq']['granularity'],
                                          ts_from=int(start_ts),
                                          ts_to=int(start_ts) + (86400 * 1e3),
                                          inverters_data=inverters_data)

        # Send remaining data
        if len(si.influxdb_data_points) > 0:
            logger.info('Sent %i points to InfluxDB server' % len(si.influxdb_data_points))
            idb_client.write_points(si.influxdb_data_points, time_precision='s')
        # Logout
        si.logout()

        logger.info('Exit program with code %i' % 0)
        sys.exit(0)

    else:
        exit_code = -1
        logger.warning('Exit program with code %i' % exit_code)
        sys.exit(exit_code)



