import os
import prefect
import requests
import re
import xmltodict

from bs4 import BeautifulSoup
from prefect import task, Flow
from typing import List, Dict, IO, Tuple

from prefect_pipeline.noaa_ndbc.models import NDBCStation, NDBCSourceStation, SourceStationConversion
from prefect_pipeline.noaa_ndbc.process_historical_data import process_and_write_files, group_files, extract_filename_info

FEED_DIR = '/Users/moriah/src/harmony/weather-pipeline/prefect_pipeline/output/feed/noaa_ndbc'
TMP_DIR =  '/Users/moriah/src/harmony/weather-pipeline/prefect_pipeline/output/tmp/noaa_ndbc'
FILE_SUFFIX = '.txt.gz'

def fetch_nbdc_url(url: str, parser):
    try:
        r = requests.get(url)
        r.raise_for_status()
        return dict(parser(r.content))
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


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


@task(name='Fetch Station list')
def fetch_station_list():
    # NDBC has data stored without station information
    # in order to get any sort of usefulness out of this data we
    # need a lookup of station id to station info (lat, lon, name, etc)
    # NOTE/TODO: If we end up  needing to reduce memory pressure
    # then I will write this to a json, but for now,we will hold it in
    # memory.
    endpoint = 'https://www.ndbc.noaa.gov/activestations.xml'
    active_stations = fetch_nbdc_url(endpoint, xmltodict.parse)
    return active_stations['stations']['station']


@task(name='Fetch Station metadata')
def fetch_station_metadata():
    # NDBC stations have a lot of pertinent metadata that is not stored
    # the main station list. (start data, end date, etc)
    # NOTE/TODO: If we end up  needing to reduce memory pressure
    # then I will write this to a json, but for now,we will hold it in
    # memory.
    endpoint = 'https://www.ndbc.noaa.gov/metadata/stationmetadata.xml'
    station_metadata = fetch_nbdc_url(endpoint, xmltodict.parse)
    return station_metadata['stations']['station']


@task(name='Fetch List of historical data urls')#, max_retries=3, retry_delay=timedelta(seconds=10))
def get_historical_stdmet_urls() -> Dict[Tuple[str, str], str]:
    # The NOAA historical data is a vastly unorganized, and sharded by station id
    # and year. The historical_stdmet_url has a list of these files in html.
    historical_urls = {}
    historical_stdmet_url = 'https://www.ndbc.noaa.gov/data/historical/stdmet'
    r = requests.get(historical_stdmet_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    for link in soup.find_all('a', attrs={'href': re.compile('^.*\.txt(\.gz)')}):
        station_link = link.get('href')
        file_info = extract_filename_info(station_link, separator='h')
        historical_urls[station_link] = historical_stdmet_url + '/' + station_link
    logger = prefect.context.get("logger")
    logger.info(f'Found {len(historical_urls)} historical urls to fetch.')
    return historical_urls


@task(name='Fetch historical data')#, max_retries=3, retry_delay=timedelta(seconds=10))
def get_historical_data_by_station(output_dir: str, historical_urls: Dict[str, str], full_pull=False) -> Dict[str, str]:
    # There are thousands of historical files ranging in size from 10kb
    # to 100mb. the structure for file naming is the only way to know what
    # station it belongs to {station_id}h{year}.txt.gz
    logger = prefect.context.get("logger")
    historical_files = {}
    for filename, url in list(historical_urls.items())[:5]: #TODO: remove this so we get all files.
        logger.info(f'Processing historical file: {filename}, {url}')
        historical_file = os.path.join(output_dir, filename)
        if full_pull:
            r = requests.get(url)        
            open(historical_file, 'wb').write(r.content)
        historical_files[filename] = historical_file
    logger.info(f'Found Historical urls: {historical_files}')
    return historical_files


@task(name='Fetch recent sdmet file urls')
def get_recent_sdmet_urls() -> Dict[str,Dict[str,str]]:
    base_url = 'https://www.ndbc.noaa.gov/data/stdmet/%s'
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    output_urls = {}
    logger = prefect.context.get("logger")
    for i, month in enumerate(months):
        url = base_url % month
        link_list_html = requests.get(url)
        soup = BeautifulSoup(link_list_html.text, 'html.parser')
        for link in soup.find_all('a', attrs={'href': re.compile('^.*\.txt(\.gz)')}):
            station_link = link.get('href')
            month_number = str(i + 1)
            station_id, year = extract_filename_info(station_link, logger, separator=month_number)
            output_urls[url + '/' + station_link] = {'station_id': station_id, 'year': year, 'month': month_number}
    return output_urls


@task(name='Fetch recent sdmet file data')
def get_recent_sdmet_data(output_dir: str, recent_urls: Dict[str,Dict[str,str]], full_pull=False) -> Dict[str,Dict[str,str]]:
    recent_files = {}
    for url, file_info in list(recent_urls.items()): #TODO: remove this so we get all files.
        station_id, year, month = file_info['station_id'], file_info['year'], file_info['month']
        filename = f'{station_id}_{year}_{month}{FILE_SUFFIX}'
        recent_file = os.path.join(output_dir, filename)
        if full_pull:
            r = requests.get(url)
            open(recent_file, 'wb').write(r.content)
        recent_files[recent_file] = file_info
    return recent_files


@task(name='Process Station metadata')
def process_active_stations(active_stations) -> Dict[str, NDBCStation]:
    # Clean the station input into the format we want specified in NDBCStation
    cleaned_stations = {}
    for station in active_stations:
        cleaned_station = convert_row_model(station)
        cleaned_stations[cleaned_station['id']] = cleaned_station
    return cleaned_stations


@task(name='Process Historical Station data')
def process_historical_data(historical_files: Dict[Tuple[str, str], str], processed_station_lookup: Dict[str, NDBCStation], output_dir: str):
    station_files_by_year = group_files(historical_files, processed_station_lookup)
    logger = prefect.context.get("logger")
    logger.info(f'Found years urls: {station_files_by_year}')
    output_year_files = process_anwd_write_files(station_files_by_year, processed_station_lookup, output_dir, logger)
    return output_year_files


@task(name='Process Recent Station data')
def process_recent_data(recent_files: Dict[str,Dict[str,str]], processed_station_lookup: Dict[str, NDBCStation], output_dir: str):
    logger = prefect.context.get("logger")
    logger.info(f'Found {len(recent_files)} recent files')
    station_files_by_year = group_files(recent_files, processed_station_lookup, logger)
    logger.info(f'Found years urls: {station_files_by_year}')
    output_year_files = process_and_write_files(station_files_by_year, processed_station_lookup, output_dir, logger)
    return output_year_files


def main():
    with Flow("NOAA NDBC Standard Meteorlogical Data") as flow:
        active_stations = fetch_station_list()
        processed_station_lookup = process_active_stations(active_stations)

        # historical_urls = get_historical_stdmet_urls()
        # historical_files = get_historical_data_by_station(os.path.join(FEED_DIR, 'historical'), historical_urls)
        recent_sdmet_urls = get_recent_sdmet_urls()
        recent_sdmet_files = get_recent_sdmet_data(os.path.join(FEED_DIR, 'historical'), recent_sdmet_urls)
        process_recent_data(recent_sdmet_files, processed_station_lookup, os.path.join(TMP_DIR, 'historical'))
        # process_historical_data(historical_files, processed_station_lookup, os.path.join(TMP_DIR, 'historical'))

    flow.run()

if __name__ == "__main__":
    main()
