import sys
import json
import pymongo
from pymongo import errors
import time
import socket
import urllib2
import logging
import logging.handlers
import os


def get_geo_data(tgt_ip):
        geo_logger.info("Gathering geo data for {0}.".format(tgt_ip))
        g = json.loads(urllib2.urlopen("http://api.petabyet.com/geoip/{0}".format(tgt_ip)).read())
        geo_data = {
                   "source": g["ip"],
                   "loc": {
                       "type": "Point",
                       "coordinates": [
                           float(g["longitude"]), float(g["latitude"])
                       ]
                   },
                   "organization": g["isp"],
                   "country": g["country"],
                   }
        return geo_data


def get_con(mhost):
    try:
        c = pymongo.MongoClient('mongodb://{0}:27017'.format(mhost))
        return c
    except pymongo.errors.ServerSelectionTimeoutError:
        print "Error connecting to MongoDB host."


def get_uniq_ips(client):
    db = client.all_logs.firewall_logs
    items = db.distinct("event source")
    return items


def valid_ip(ip):
    try:
        socket.inet_aton(ip)
        return True
    except:
        return False


def log_it(**kwargs):
    logger = logging.getLogger(kwargs['name'])
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    if kwargs.has_key('console'):
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    elif kwargs.has_key('rotate'):
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
    con = get_con("192.168.1.22")
    geo_logger.info("Getting unique ip's to enrich.")
    for ip in get_uniq_ips(con):
        try:
            if len(ip.split(".")) == 4:
                if valid_ip(ip) and not ip.startswith("192", 0, 3):
                    con.all_logs.ip_geodata.insert(get_geo_data(ip))
        except:
            geo_logger.info("{0} is not a valid IP.".format(ip))
        time.sleep(1)


logger_path = '/Users/rohara/development'
geo_logger = log_it(logname='ip_geoenrich.log', logpath=logger_path, name='Geo Log', rotate='')


if __name__ == "__main__":
    sys.exit(main())