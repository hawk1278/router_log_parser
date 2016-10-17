#!/bin/env python

import logging
import pymongo
import logging.handlers
import sys
import time
import os
import json
from pymongo import errors
from optparse import OptionParser


# mhost = '127.0.0.1'
# mport = '27017'

# TEST

def status_parser(logline):
    if 'ACCEPT' in logline:
        return 'ACCEPT'
    elif 'DROP' in logline:
        return 'DROP'
    elif 'WEBMON' in logline:
        return 'WEBMON'


def line_parser(logline):
    for line in logline:
        if 'SRC' in line:
            return line.split('=')[1]
        elif 'DST' in line:
            return line.split('=')[1]


def dst_parser(logline):
    for line in logline:
        if 'DST' in line:
            return line.split('=')[1]


def write_log_data(jevents):
    log_date = time.strftime('%Y-%m-%d_%H:%M:%S')
    with open('jsonlog_' + log_date + '.log', 'a') as j:
        for jevent in jevents:
            j.write(str(jevent))


def gen_events_stream(sys_log_file):
    sys_log_file.seek(0, 2)
    while True:
        line = sys_log_file.readline()
        if not line:
            time.sleep(0.1)
            continue
        yield line


def bulk_load_records_mongo(events):
    client = pymongo.MongoClient('mongodb://192.168.1.239:27017')
    db = client.all_logs
    db.firewall_logs.insert([x for x in events])
    print db.firewall_logs.count()


def load_records_mongo(json_record):
    try:
        client = pymongo.MongoClient('mongodb://{0}:{1}'.format(mhost, mport))
    except errors.ConnectionFailure as e:
        print "Unable to connect to MongoDB host at {0}".format(mhost)
        sys.exit(1)
    db = client.all_logs.firewall_logs
    post_id = db.insert(json_record)
    router_parser_logger.info('Load record with ID: {0}'.format(post_id))


def get_geo_data(src, dst):
    try:
        client = pymongo.MongoClient("mongodb://{0}:{1}".format(mhost, mport))
    except errors.ConnectionFailure as e:
        print "Unable to connect to MongoDB host at {0}".format(mhost)
        sys.exit(1)
    db = client.all_logs.ip_geodata
    src_cur = db.find({"source": src})
    dst_cur = db.find({"source": dst})
    src_data = [ x for x in src_cur]
    dst_data = [ x for x in dst_cur]
    print {"src": src_data, "dst": dst_data}

    # return {"src": src_data, "dst": dst_data}


def gen_events(fh):
    for line in fh:
        json_data = json.loads(line)
        message = json_data['message'].split(' ')
        router_parser_logger.info("Generating event.")
        status = status_parser(message)
        src_ip = [source.split("=")[1] for source in message if "SRC" in source][0]
        dst_ip = [dest.split("=")[1] for dest in message if "DST" in dest][0]
        event = {'event date': json_data['timestamp'],
                 'event status': status,
                 'event source': src_ip,
                 'event dest': dst_ip,
                 'srcorg': {},
                 'srcloc': {},
                 'dstorg': {},
                 'dstloc': {}
                 }
        yield event


def log_it(**kwargs):
    logger = logging.getLogger(kwargs['name'])
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    if 'console' in kwargs:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    elif 'rotate' in kwargs:
        rlh = logging.handlers.RotatingFileHandler(os.path.join(kwargs['logpath'], kwargs['logname']), maxBytes=102400,
                                                   backupCount=100)
        rlh.setLevel(logging.DEBUG)
        rlh.setFormatter(formatter)
        logger.addHandler(rlh)
    else:
        logging.basicConfig(filename=os.path.join(kwargs['logpath'], kwargs['logname']),
                            format='%(aasctime)s - %(name) - %(message)s')
    return logger


def main():
    global mhost
    global mport
    parser = OptionParser()
    parser.add_option('-f', '--file', dest='filename', help='Log file to parse.')
    parser.add_option('--dbhost', dest='mhost', help='MongoDB database (defaults to localhost).')
    parser.add_option('--dbport', dest='mport', help='MongoDB port (defaults to 27017).')
    (options, args) = parser.parse_args()
    if not options.filename:
        parser.print_help()
        router_parser_logger.error('No log file provided.  Exiting.')
        sys.exit(1)
    if not options.mhost:
        router_parser_logger.info('No MongoDB host provided, using localhost.')
        mhost = '127.0.0.1'
    else:
        mhost = options.mhost
        router_parser_logger.info('Setting MongoDB host to {0}'.format(mhost))
    if not options.mport:
        router_parser_logger.info('No MongoDB port provided, using 27017.')
        mport = '27017'
        router_parser_logger.info('Setting MongoDB port to {0}'.format(mport))
    else:
        mport = options.mport

    filename = options.filename
    router_parser_logger.info('File to parse: {0}'.format(filename))

    try:
        c = open(filename)
    except IOError:
        print "Unable to open {0}.  Exiting application".format(filename)
        sys.exit(1)

    router_parser_logger.info('Starting router log parser application.')
    json_log_lines = gen_events_stream(c)
    json_events = gen_events(json_log_lines)

    for json_event in json_events:
        load_records_mongo(json_event)


logger_path = '/home/rich/router_log_parser/logs'
log_name='router_log_parser.log'
try:
    os.makedirs(logger_path, 0777)
except OSError as exc:
        if os.path.isdir(logger_path):
            pass
        else:
            raise

router_parser_logger = log_it(logname=log_name, logpath=logger_path, name='Router Logs', rotate='')
os.chmod(os.path.join(logger_path,log_name), 0777)

if __name__ == '__main__':
    sys.exit(main())
