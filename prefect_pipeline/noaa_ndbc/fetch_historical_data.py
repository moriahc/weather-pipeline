#!/usr/bin/env python
import sys
import requests
import re

from bs4 import BeautifulSoup
from pylib.base.flags import Flags

def scrape_data_by_station(output_dir: str):
    url = 'https://www.ndbc.noaa.gov/historical_data.shtml'
    prefix = '/download_data.php?filename='
    suffix = '&dir=data/historical/stdmet/'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    for link in soup.find_all('a', attrs={'href': re.compile("^/download_data.php*")}):
        station_link = link.get('href')
        filename = station_link.replace(prefix, '').replace(suffix, '')
        source_url = f'https://www.ndbc.noaa.gov/data/historical/stdmet/{filename}'
        print(f'Fetching file {source_url}')
        output_file = output_dir + '/' +  filename
        r = requests.get(source_url)
        open(output_file, 'wb').write(r.content)

def main():
    Flags.PARSER.add_argument(
        '--output_file_directory',
        type=str,
        required=True,
        help='Path to directory to write fetched historical data',
    )
    Flags.InitArgs()
    scrape_data_by_station(Flags.ARGS.output_file_directory)
    return 0


if __name__ == "__main__":
    sys.exit(main())
