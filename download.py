import requests
import re
import numpy as np
from os import path, makedirs, listdir
from bs4 import BeautifulSoup
from zipfile import ZipFile
from csv import reader
from io import TextIOWrapper


class DataDownloader:
    """Class for data fetching from server"""

    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="data", cache_filename="data_{}.pkl.gz"):
        """Init method checks if directory exists in other case, tra to make it"""
        if not path.exists(folder):
            try:
                makedirs(folder)
            except OSError as e:
                print(f'Could not create directory "{folder}" with error: {e}')
                exit(1)
        self.folder = folder
        self.url = url
        self.cache_filename = cache_filename
        # add headers to look like browser
        self.headers = {
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 '
                          'Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,'
                      '*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': '?1',
            'Sec-Fetch-Dest': 'document',
            'Referer': 'https://ehw.fit.vutbr.cz/izv/',
            'Accept-Language': 'cs-CZ,cs;q=0.9,en;q=0.8',
        }
        self.region_files = {
            "PHA": "00.csv",
            "STC": "01.csv",
            "JHC": "02.csv",
            "PLK": "03.csv",
            "ULK": "04.csv",
            "HKK": "05.csv",
            "JHM": "06.csv",
            "MSK": "07.csv",
            "OLK": "14.csv",
            "ZLK": "15.csv",
            "VYS": "16.csv",
            "PAK": "17.csv",
            "LBK": "18.csv",
            "KVK": "19.csv",
            "XXX": "CHODCI.csv"
        }
        self.csv_headers = [{"label": "p1", "d_type": "<U12"}, {"label": "p36", "d_type": "i1"},
                            {"label": "p37", "d_type": "i1"}, {"label": "p2a", "d_type": "<U10"},
                            {"label": "weekday(p2a)", "d_type": "i1"}, {"label": "p2b", "d_type": "<U4"},
                            {"label": "p6", "d_type": "i1"}, {"label": "p7", "d_type": "i1"},
                            {"label": "p8", "d_type": "i1"}, {"label": "p9", "d_type": "i1"},
                            {"label": "p10", "d_type": "i1"}, {"label": "p11", "d_type": "i1"},
                            {"label": "p12", "d_type": "i2"}, {"label": "p13a", "d_type": "i1"},
                            {"label": "p13b", "d_type": "i1"}, {"label": "p13c", "d_type": "i1"},
                            {"label": "p14", "d_type": "i8"}, {"label": "p15", "d_type": "i1"},
                            {"label": "p16", "d_type": "i1"}, {"label": "p17", "d_type": "i1"},
                            {"label": "p18", "d_type": "i1"}, {"label": "p19", "d_type": "i1"},
                            {"label": "p20", "d_type": "i1"}, {"label": "p21", "d_type": "i1"},
                            {"label": "p22", "d_type": "i1"}, {"label": "p23", "d_type": "i1"},
                            {"label": "p24", "d_type": "i1"}, {"label": "p27", "d_type": "i1"},
                            {"label": "p28", "d_type": "i1"}, {"label": "p34", "d_type": "i2"},
                            {"label": "p35", "d_type": "i1"}, {"label": "p39", "d_type": "i1"},
                            {"label": "p44", "d_type": "i1"}, {"label": "p45a", "d_type": "i1"},
                            {"label": "p47", "d_type": "i1"}, {"label": "p48a", "d_type": "i1"},
                            {"label": "p49", "d_type": "i1"}, {"label": "p50a", "d_type": "i1"},
                            {"label": "p50b", "d_type": "i1"}, {"label": "p51", "d_type": "i1"},
                            {"label": "p52", "d_type": "i1"}, {"label": "p53", "d_type": "i1"},
                            {"label": "p55a", "d_type": "i1"}, {"label": "p57", "d_type": "i1"},
                            {"label": "p58", "d_type": "i1"},
                            {"label": "a", "d_type": "i1"}, {"label": "b", "d_type": "i1"},
                            {"label": "d", "d_type": "i1"}, {"label": "e", "d_type": "f8"},
                            {"label": "f", "d_type": "f8"}, {"label": "g", "d_type": "i1"},
                            {"label": "h", "d_type": "i1"}, {"label": "i", "d_type": "i1"},
                            {"label": "j", "d_type": "i1"}, {"label": "k", "d_type": "i1"},
                            {"label": "l", "d_type": "i1"}, {"label": "n", "d_type": "i1"},
                            {"label": "o", "d_type": "i1"}, {"label": "p", "d_type": "i1"},
                            {"label": "q", "d_type": "i1"}, {"label": "r", "d_type": "i1"},
                            {"label": "s", "d_type": "i1"}, {"label": "t", "d_type": "i1"},
                            {"label": "p5a", "d_type": "i1"},
                            ]

    def __get_file_paths_from_url(self):
        """Get all dataset paths on specified url"""
        response = requests.get(self.url, headers=self.headers)
        if response.status_code != 200:
            print(f'Could not fetch url "{self.url}"')
            exit(1)

        # get html and parse all a tags with .zip files
        response_html = response.text
        soup = BeautifulSoup(response_html, "html.parser")
        a_tags = soup.table.find_all('a', {'href': re.compile(r'\.zip')})
        # return only links without a tags
        return [a_tag.attrs["href"] for a_tag in a_tags]

    def __get_existing_datasets(self):
        """Get all dataset names available in cwd"""
        files_in_directory = listdir(self.folder)
        return [file for file in files_in_directory if file.endswith(".zip")]

    def __download_missing_files(self):
        """Detect any missing file in cwd"""
        # regex to match only zip files
        re_name = re.compile(r'[^/]*\.zip')
        # get all available dataset paths in table on specified url and theirs names
        file_paths = self.__get_file_paths_from_url()
        # get all name of files in our cwd
        existing_file_names = self.__get_existing_datasets()
        for item in file_paths:
            file_name = re_name.search(item).group(0)
            if file_name not in existing_file_names:
                self.__download_missing_file(f'{self.url}{item}', file_name)

    def __download_missing_file(self, file_path, file_name):
        """ Download data in stream mode for faster processing"""
        response = requests.get(f'{self.url}{file_path}', headers=self.headers, stream=True)
        # we check for status code before setting data
        if response.status_code != 200:
            print(f'Could not fetch {self.url}{file_path}')
        else:
            # write to output file in chunks for faster
            with open(f'{self.folder}/{file_name}', 'wb+') as fd:
                for chunk in response.iter_content(chunk_size=128):
                    fd.write(chunk)

    def download_data(self):
        """Download all datasets available at specified url"""
        # regex to match only zip files
        re_name = re.compile(r'[^/]*\.zip')
        file_paths = self.__get_file_paths_from_url()
        for file_path in file_paths:
            file_name = re_name.search(file_path).group(0)
            self.__download_missing_file(file_path, file_name)

    def __parse_csv_file(self, file):
        """Parse single csv file and return list with numpy arrays"""
        number_of_rows = sum(1 for _ in file)
        parsed_data = [np.empty(shape=number_of_rows, dtype=item['d_type']) for item in self.csv_headers]
        file.seek(0)
        csv_reader = reader(TextIOWrapper(file, "ISO-8859-1"), delimiter=';', quotechar='"')
        for index, row in enumerate(csv_reader):
            for index_val, value in enumerate(row):
                try:
                    parsed_data[index_val][index] = value
                except ValueError:
                    parsed_data[index_val][index] = -1
        return parsed_data

    def parse_region_data(self, region):
        """Parse data for current region to tuple(list[str], list[np.ndarray])"""
        self.__download_missing_files()
        datasets = self.__get_existing_datasets()
        parsed_data = [np.ndarray(shape=(0,), dtype=item['d_type']) for item in self.csv_headers]
        for dataset in datasets:
            with ZipFile(f'{self.folder}/{dataset}') as archive:
                try:
                    with archive.open(self.region_files[region], 'r') as file:
                        parsed_data_to_merge = self.__parse_csv_file(file)
                        for index, value in enumerate(parsed_data_to_merge):
                            parsed_data[index] = np.concatenate([parsed_data[index], value])
                except KeyError:
                    print(f'Provided region key does not exist. {region} will be skipped.')
        return [item['label'] for item in self.csv_headers], parsed_data

    def get_list(self, regions=None):
        if regions is None:
            regions_parsed = [self.parse_region_data(region) for region in self.region_files]
        else:
            regions_parsed = [self.parse_region_data(region) for region in regions]
        print(len(regions_parsed))
