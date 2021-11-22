#!/usr/bin/env python
import csv
import sys
import os
import pandas as pd
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, TypedDict, Tuple, Set
from bs4 import BeautifulSoup

from compression.pigz import PigzReader, PigzWriter
from prefect_pipeline.noaa_ndbc.models import NDBCStation, StandardMeteorologicalDataConversionPost2006, StandardMeteorologicalDataConversionPre2006


def extract_filename_info(filename: str, logger, separator='h') -> Tuple[str, str]:
    suffix = '.txt.gz'
    raw_name = filename.replace(suffix, '')
    # has to be 4 digits
    year = raw_name[-4:]
    raw_name = raw_name[:-4]
    if raw_name.endswith(separator):
        station_id = raw_name[:-len(separator)]
        return station_id, year
    raise ValueError(f'File name format is invalid {filename}, {separator}')
    # offset = -4-separator
    # assert raw_name[offset:-4] == separator
    # if historical:
    #     # NOAA adds an h in between the station id and the year for historical data
    #     assert separator == 'h', f'Thats weird {separator}, {filename}'
    # else:
    #     # NOAA adds the month int in between the station id and the year for historical data
    #     # TODO-URGENT once the recent 
    #     assert int(separator) in set(range(1, 10)), f'Thats weird {separator}, {filename}'

    # return raw_name[:-5], year

def build_station_id_list(filepath: str) -> Dict[str, NDBCStation]:
    # Load the stations into a dictionary lookup of stations id to NDBCStation
    station_lookup = {}
    with open(filepath, 'r') as input_stations:
        station_reader = csv.DictReader(input_stations)
        for station in station_reader:
            station_lookup[station['id']] = station
    return station_lookup

def get_year_specific_parsing(year):
    year_number = int(year)
    if year_number < 1999:
        return [['YY', "MM", 'DD', 'hh']], lambda x: datetime.strptime(x, "%y %m %d %H"), StandardMeteorologicalDataConversionPre2006
    if year_number < 2005:
        return [['YYYY', "MM", 'DD', 'hh']], lambda x: datetime.strptime(x, "%Y %m %d %H"), StandardMeteorologicalDataConversionPre2006
    if year_number < 2007:
        return [['YYYY', "MM", 'DD', 'hh', 'mm']], lambda x: datetime.strptime(x, "%Y %m %d %H %M"), StandardMeteorologicalDataConversionPre2006
    if year_number >= 2007:
        return [['#YY', "MM", 'DD', 'hh', 'mm']], lambda x: datetime.strptime(x, "%Y %m %d %H %M"), StandardMeteorologicalDataConversionPost2006
    print('oops')

def process_file(station, i, year, station_lookup, ignore_first_row, null_values, year_to_process_as, total_stations, logger):
    name, filepath = station
    parse_dates, date_parser, Conversion = get_year_specific_parsing(year_to_process_as)
    dimensions = station_lookup[name]

    print(f'processing {name}---{filepath}: {i + 1}/{total_stations}')
    # we need one dimension to join on.
    # I think most location dimenison are really lat/lon so that's what we will use
    # All of these files are relatively small ~10kb and can easily be loaded into a df
    # row 1 is the units.
    skip_rows = [1] if ignore_first_row[year_to_process_as] else None
    try:
        data_df = pd.read_csv(
            filepath,
            compression='gzip',
            delimiter='\s+',
            skiprows=skip_rows,
            parse_dates=parse_dates,
            date_parser=date_parser,
        )
        # Load in the station data rename columns/fields
        # I want to pivot the data into a feild value system
        # this will allow for a more dynamic table and more diverse amount
        # of field value
        # We also need to parse out null values

        date_rename = {'#YY_MM_DD_hh_mm': 'datetime_utc', 'YY_MM_DD_hh_mm': 'datetime_utc', 'YYYY_MM_DD_hh_mm': 'datetime_utc', '#YY_MM_DD_hh': 'datetime_utc', 'YY_MM_DD_hh': 'datetime_utc', 'YYYY_MM_DD_hh': 'datetime_utc'}
        data_df = data_df.rename(columns=Conversion)
        data_df = data_df.rename(columns=date_rename)
        data_df = data_df.melt(
            id_vars=['datetime_utc'],
            value_vars=list(Conversion.values()),
            var_name='field',
            value_name='val',
        )
        # filter out null values
        data_df = data_df[~data_df['val'].isin(null_values)]
        # Add dimension data
        for d in dimensions:
            data_df[d] = str(dimensions[d])
        return data_df
    except ValueError as v_err:
        print(v_err)
        next_year = int(year_to_process_as) + 1
        if next_year > 2009:
            raise ValueError(v_err)
        print(f'Issue with file {name}, {year}: trying as {next_year} format')
        return process_file(station, i, year, station_lookup, ignore_first_row,  null_values, str(next_year), total_stations)
    except TypeError as t_err:
        print(t_err)
        next_year = int(year_to_process_as) + 1
        if next_year > 2009:
            raise TypeError(t_err)
        print(f'Issue with file {name}, {year}: trying as {next_year} format')
        return process_file(station, i, year, station_lookup, ignore_first_row,  null_values, str(next_year), total_stations)
    except pd.errors.EmptyDataError:
        logger.warning(f'Empty file found for {station}')
        return pd.DataFrame([])

def process_and_write_files(
    stations_by_year: Dict[str, Tuple[str, str]],
    station_lookup: Dict[str, NDBCStation],
    output_directory: str,
    logger,
):
    ignore_first_row = { str(i): i >= 2007 for i in range(1970, 2022)}
    null_values = set([99, 99.0, 99.00, 999, 999.0, 999.00, 9999.0,])
    # print(f'Starting row processing: {stations_by_year.keys()}')
    output_files = {}
    for year in stations_by_year:
        total_stations = len(stations_by_year[year])
        logger.info(f'Processing year {year}, {total_stations}')
        # print(f'Processing year {year}, {total_stations}')
        output_file_path = os.path.join(output_directory, f'processed_rows.{year}.json.gz')
        output_files[year] = output_file_path
        with PigzWriter(output_file_path) as output_file:
            for i, station in enumerate(stations_by_year[year]):
                data_df = process_file(station, i, year, station_lookup, ignore_first_row, null_values, year, total_stations, logger)
                # logger.info(f'DATA DF: {data_df.head()}')
                data_df.to_json(output_file, orient='records', date_format='iso')
    return output_files


def group_files(files: Dict[str,Dict[str,str]], station_lookup: Dict[str, NDBCStation], logger) -> Dict[str, Set[Tuple[str, str]]]:
    stations_by_year = defaultdict(set)
    for file_path, file_info in files.items():
        # Extract the station id and year from the title
        station_id, year, month = file_info['station_id'], file_info['year'], file_info['month']
        if station_id in station_lookup:
            # Filter out stations we have no station metadata for.
            # A half data point isn't useful. without a location its just non-sense
            stations_by_year[year].add((station_id, file_path))
        else:
            logger.warning(f'Station not found: {station_id} {year} -- {file_path}')
    return dict(stations_by_year)

