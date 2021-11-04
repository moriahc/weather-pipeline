#!/usr/bin/env python
import sys
import requests
import csv
from typing import Dict, List, Any, TypedDict
from doodads import get_useful_thing
from util import write_output_csv


REQUEST_SIZE = 1000

class NCDCStation(TypedDict):
    datacoverage: float
    elevation: float
    elevationUnit: str
    id: str
    latitude: float
    longitude: float
    maxdate: str
    mindate: str
    name: str

class NCDCDataset(TypedDict):
    datacoverage: float
    id: str
    maxdate: str
    mindate: str
    name: str
    uid: float

class NCDCDataRow(TypedDict):
    date: str
    datatype: str
    station: str
    attributes: str
    value: float

def fetch_ncdc_url(url: str) -> List[NCDCStation]:
    token = get_useful_thing('ncdc_noaa')
    try:
        r = requests.get(url, headers={f'token': token})
        r.raise_for_status()
        response = r.json()
        print(url)
        print(response)
        return response['metadata']['resultset']['count'], response['results']
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

def fetch_with_paging(base_url, limit=REQUEST_SIZE) -> Any:
    next_page = True
    offset = 0
    results = []
    while next_page:
        paging_url = 'limit={limit}&offset={offset}'.format(limit=limit, offset=offset)
        url = base_url + paging_url
        print(f'Starting fetch for {url}')
        count, result = fetch_ncdc_url(url)
        results.extend(result)
        offset += limit
        next_page = offset < count
    return results

def fetch_stations() -> List[NCDCStation]:
    base_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/stations?'
    return fetch_with_paging(base_url)

def fetch_datasets() -> List[NCDCDataset]:
    base_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/datasets?'
    return fetch_with_paging(base_url)

def fetch_data(stations: str, dataset: NCDCDataset, station_shard_size: int):
    dataset_id, start, end = dataset['id'], dataset['mindate'], dataset['maxdate']
    station_id_string = '&stationid='.join(stations)
    base_url = f'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid={dataset_id}&stationid={station_id_string}&startdate={start}&enddate=end&'
    return fetch_with_paging(base_url)

def main():
    stations = fetch_stations()
    stations_written = write_output_csv('ncdc_stations.csv', stations, list(NCDCStation.__annotations__.keys()))
    datasets = fetch_datasets()
    datasets_written = write_output_csv('ncdc_datasets.csv', datasets, list(NCDCDataset.__annotations__.keys()))
    station_ids = [station['id'] for station in station_reader][:10]
    datasets = [d for d in datasets_reader]
    for dataset in [datasets[0]]: #remove
        data = fetch_data(station_ids, dataset)
    return 1


if __name__ == "__main__":
    sys.exit(main())
