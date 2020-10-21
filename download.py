import requests
import re
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
        self.csv_headers = ["p1", "p36", "p37", "p2a", "weekday(p2a)", "p2b", "p6", "p7", "p8", "p9", "p10", "p11",
                            "p12", "p13a", "p13b", "p13c", "p14", "p15", "p16", "p17", "p18", "p19", "p20", "p21",
                            "p22", "p23", "p24", "p27", "p28", "p34", "p35", "p39", "p44", "p45a", "p47", "p48a", "p49",
                            "p50a", "p50b", "p51", "p52", "p53", "p55a", "p57", "p58", "a", "b", "d", "e", "f", "g",
                            "h", "i", "j", "k", "l", "n", "o", "p", "q", "r", "s", "t", "p5a"]

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

    def __parse_csv_file(self, file, number_of_params):
        rows = sum(1 for _ in file)
        print(rows)
        # csv_reader = reader(TextIOWrapper(file, "ISO-8859-1"), delimiter=';', quotechar='|')

    def parse_region_data(self, region):
        """Parse data for current region to tuple(list[str], list[np.ndarray])"""
        self.__download_missing_files()
        datasets = self.__get_existing_datasets()
        length_of_headers = len(self.csv_headers)
        print(length_of_headers, "fu")
        parsed_data = (self.csv_headers, [])
        for dataset in datasets:
            archive = ZipFile(f'{self.folder}/{dataset}', 'r')
            try:
                with archive.open(self.region_files[region]) as file:
                    self.__parse_csv_file(file, length_of_headers)
            except KeyError:
                print('Provided region does not exists!')
                exit(1)
