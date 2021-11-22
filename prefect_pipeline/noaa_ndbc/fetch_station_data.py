#!/usr/bin/env python
import sys
import requests
import csv
import re
import xmltodict
import pandas as pd
from util import write_output_csv
from bs4 import BeautifulSoup
from models import NBDCStation, NBDCSourceStation

def scrape_data_by_station(output_dir):
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
    scrape_data_by_station('out_ndbc')
    return 1


if __name__ == "__main__":
    sys.exit(main())

https://www.ndbc.noaa.gov/data/stdmet/Jan/4600612021.txt.gz