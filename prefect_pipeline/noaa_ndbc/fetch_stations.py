#!/usr/bin/env python
import sys
import requests
import json
import xmltodict

from pylib.base.flags import Flags

from models import NDBCSourceStation
from typing import List

def fetch_nbdc_url(url: str) -> List[NDBCSourceStation]:
    try:
        r = requests.get(url)
        r.raise_for_status()
        return dict(xmltodict.parse(r.content))
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


def main():
    Flags.PARSER.add_argument(
        '--output_stations_file',
        type=str,
        required=True,
        help='File path for fetched stations json',
    )
    Flags.PARSER.add_argument(
        '--output_stations_metadata_file',
        type=str,
        required=True,
        help='File path for fetched station metadata json',
    )
    Flags.InitArgs()
    active_station_list = fetch_nbdc_url('https://www.ndbc.noaa.gov/activestations.xml')
    historical_station_metadata = fetch_nbdc_url('https://www.ndbc.noaa.gov/metadata/stationmetadata.xml')
    with open(Flags.ARGS.output_stations_file, 'w') as output_stations, \
        open(Flags.ARGS.output_stations_metadata_file, 'w') as output_stations_metadata:
        json.dump(active_station_list['stations']['station'], output_stations)
        json.dump(historical_station_metadata['stations']['station'], output_stations_metadata)
    return 0


if __name__ == "__main__":
    sys.exit(main())
