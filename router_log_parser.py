import logging
import logging.handlers
import os
import time
import sys
import json
import pymongo
from optparse import OptionParser


def status_parser(logline):
    if 'ACCEPT' in logline:
        return 'ACCEPT'
    elif 'DROP' in logline:
        return 'DROP'
    elif 'WEBMON' in logline:
        return 'WEBMON'


def src_parser(logline):
    for line in logline:
        if 'SRC' in line:
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
    # router_parser_logger.info('Begining read from {0}.'.format(sys_log_file))
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


def load_records_mongo(json_record, mhost):
    client = pymongo.MongoClient('mongodb://{0}:27017'.format(mhost))
    db = client.all_logs.firewall_logs
    post_id = db.insert(json_record)
    router_parser_logger.info('Load record with ID: {0}'.format(post_id))


def gen_events(fh):
    for line in fh:
        json_data = json.loads(line)
        message = json_data['message'].split(' ')
        router_parser_logger.info("Generating event.")
        event = {'event date': json_data['timestamp'],
                 'event status': status_parser(message),
                 'event source': src_parser(message),
                 'event dest': dst_parser(message)
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
    parser = OptionParser()
    parser.add_option('-f', '--file', dest='filename', help='Log file to parse.')
    (options, args) = parser.parse_args()
    if not options.filename:
        parser.print_help()
        router_parser_logger.info('No log file provided.  Exiting.')
        sys.exit(1)
    filename = options.filename
    router_parser_logger.info('File to parser: {0}'.format(filename))
    c = open(filename)
    json_log_lines = gen_events_stream(c)
    json_events = gen_events(json_log_lines)
    mongo_host = '127.0.0.1'
    for json_event in json_events:
        load_records_mongo(json_event, mongo_host)


logger_path = '/home/rich/development/router_log_parser/logs'
router_parser_logger = log_it(logname='router_log_parser.log', logpath=logger_path, name='Router Logs', rotate='')

if __name__ == '__main__':
    sys.exit(main())
