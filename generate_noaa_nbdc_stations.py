#!/usr/bin/env python
import sys
import requests
import csv
from typing import Dict, List, Any, TypedDict
import xmltodict
import pandas as pd
from util import write_output_csv

REQUEST_SIZE = 1000

class NBDCStation(TypedDict):
    id: str
    latitude: float
    longitude: float
    # elev: float
    name: str
    owner: str
    program: str
    type: str
    met: bool
    currents: bool
    water_quality: bool
    dart: bool

class NBDCSourceStation(TypedDict):
    id: str
    lat: str
    lon: str
    # elev: str
    name: str
    owner: str
    pgm: str
    type: str
    met: str
    currents: str
    waterquality: str
    dart: str

def fetch_nbdc_url(url: str) -> List[NBDCSourceStation]:
    try:
        r = requests.get(url)
        r.raise_for_status()
        return dict(xmltodict.parse(r.content))
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

def convert_row_model(row, source_model, output_model) -> NBDCStation:
    output_row = {}
    source_fields = list(NBDCSourceStation.__annotations__.items())
    output_fields = list(NBDCStation.__annotations__.items())
    for i, field in enumerate(source_fields):
        source_field, source_type = field
        value = row.get(f'@{source_field}')
        output_field, output_type = output_fields[i]
        output_row[output_field] = output_type(value)
    return output_row

def process_nbdc_station_list(stations: List[NBDCSourceStation]) -> List[NBDCStation]:
    # Process everything frrom the source type to the actual type 
    output_stations = []
    source_fields = list(NBDCSourceStation.__annotations__.items())
    output_fields = list(NBDCStation.__annotations__.items())
    for station in stations:
        output_stations.append(convert_row_model(station, NBDCSourceStation, NBDCStation))
    return output_stations

def print_stats(stations):
    station_df = pd.DataFrame(stations)
    types = station_df['type'].unique()
    unique_ids = len(station_df['id'].unique())
    current_count = len(station_df[station_df['currents']])
    met_count = len(station_df[station_df['met']])
    water_quality_count = len(station_df[station_df['water_quality']])
    dart_count = len(station_df[station_df['dart']])
    print(f'Found {len(station_df)} total active stations')
    print(f'Types: {types}')
    print(f'Has Currents: {current_count}')
    print(f'Has Met: {met_count}')
    print(f'Has Water Quality: {water_quality_count}')
    print(f'Has dart: {dart_count}')

def main():
    active_station_list = fetch_nbdc_url('https://www.ndbc.noaa.gov/activestations.xml')
    processed_stations = process_nbdc_station_list(active_station_list['stations']['station'])
    print_stats(processed_stations)
    write_output_csv('ndbc_station_list.csv', processed_stations, list(NBDCStation.__annotations__.keys()))

    return 1


if __name__ == "__main__":
    sys.exit(main())
