import geocoder
import sys
import json


def get_geo_data(tgt_ip):
    g = geocoder.maxmind(tgt_ip).json
    print g
    sys.exit(1)
    geo_data = {
               "lat": g["lat"],
               "long": g["lng"],
               "organization": g["org"],
               "postal code": g["postal"],
               "address": g["address"],
               "hostname": g["hostname"]
               }
    return json.dumps(geo_data)


def main():
    print get_geo_data('204.79.197.200')


if __name__ == "__main__":
    sys.exit(main())
