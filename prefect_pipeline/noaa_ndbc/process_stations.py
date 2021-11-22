#!/usr/bin/env python
import sys
import csv
import json

from typing import List, IO
from pylib.base.flags import Flags
from models import NDBCStation, NDBCSourceStation, SourceStationConversion

def convert_row_model(row: NDBCSourceStation) -> NDBCStation:
    output_row = {}
    for field, value in row.items():
        output_field = SourceStationConversion.get(field)
        if not output_field:
            continue
        output_type = NDBCStation.__annotations__[output_field]
        if output_type is not bool:
            output_row[output_field] = output_type(value)
        else:
            output_row[output_field] = value == 'y'
    return output_row


def write_ndbc_station(input_stations: IO, output_stations: IO) -> List[NDBCStation]:
    station_writer = csv.DictWriter(output_stations, fieldnames=list(SourceStationConversion.values()))
    station_writer.writeheader()
    stations = json.load(input_stations)
    station_types = set()
    station_ids = set()
    for station in stations:
        processed_station = convert_row_model(station)
        station_types.add(processed_station['type'])
        station_ids.add(processed_station['id'])
        station_writer.writerow(processed_station)
    print(f'Found {len(station_ids)} total active stations')
    print(f'Types: {station_types}')


def main():
    Flags.PARSER.add_argument(
        '--input_stations_file',
        type=str,
        required=True,
        help='File path for input stations csv',
    )
    Flags.PARSER.add_argument(
        '--intput_stations_metadata_file',
        type=str,
        required=True,
        help='Filepath for input station metadata',
    )
    Flags.PARSER.add_argument(
        '--output_stations_file',
        type=str,
        required=True,
        help='Filepath for input station metadata',
    )
    Flags.InitArgs()
    with open(Flags.ARGS.input_stations_file, 'r') as input_stations, \
        open(Flags.ARGS.output_stations_file, 'w') as output_stations:
        write_ndbc_station(input_stations, output_stations)
        # TODO(moriah): use station metadata

    return 0


if __name__ == "__main__":
    sys.exit(main())
