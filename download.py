import numpy as np
import pickle
import re

from gzip import open as open_gzip
from requests import get as get_request
from os import path, makedirs, listdir, remove as remove_file
from bs4 import BeautifulSoup
from zipfile import ZipFile
from csv import reader
from io import TextIOWrapper


class DataDownloader:
    """Class for fetching and parsing data about car accidents in Czech republic"""

    def __init__(self, url="https://ehw.fit.vutbr.cz/izv/", folder="data", cache_filename="data_{}.pkl.gz"):
        """Init method checks if directory exists in other case, tra to make it"""
        # check for valid paths
        if re.match(r'[^-_.A-Za-z0-9/]', folder):
            raise OSError(f'Provided folder path is not posixly correct relative path: {cache_filename}')
        if re.match(r'[^-_.A-Za-z0-9{}]', cache_filename):
            raise OSError(f'Provided cache_filename is not posixly correct : {cache_filename}')
        if not path.exists(folder):
            try:
                makedirs(folder)
            except OSError:
                raise OSError(f'Could not create directory: {folder}')
        self.folder = folder

        # check if cache_filename is correct
        if re.fullmatch(r'[^{}/]*{}[^{}/]*\.pkl\.gz', cache_filename):
            self.cache_filename = cache_filename
        else:
            raise ValueError(
                f'Provided cache file_name could not be formatted, or file type is not .pkl.gz: {cache_filename} ')

        # add attribute for datasets that needs to be parsed
        self.parsed_data = {}
        self.parsed_regions = []
        self.non_duplicate_datasets = None

        # add headers to look like browser
        self.url = url
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

        # regions settings and csv headers
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
        }
        self.csv_headers = [{"label": "p1", "d_type": "i8"}, {"label": "p36", "d_type": "i1"},
                            # IDENTIFIKAČNÍ ČÍSLO (0-999 999 999 999), DRUH POZEMNÍ KOMUNIKACE (0-8)
                            {"label": "p37", "d_type": "i4"}, {"label": "p2a", "d_type": "datetime64[D]"},
                            # ČÍSLO POZEMNÍ KOMUNIKACE (0-999 999), ČASOVÉ ÚDAJE O DOPRAVNÍ NEHODĚ (YYYY-MM-DD)
                            {"label": "weekday(p2a)", "d_type": "i1"}, {"label": "p2b", "d_type": "i2"},
                            # DEN TÝDNE (0-7),
                            # ČASOVÉ ÚDAJE O DOPRAVNÍ NEHODĚ (HHMM, 25xx - unspecified hour, xx60, unspecified minute)
                            {"label": "p6", "d_type": "i1"}, {"label": "p7", "d_type": "i1"},
                            # DRUH NEHODY (0-9), DRUH SRÁŽKY JEDOUCÍCH VOZIDEL (0-4)
                            {"label": "p8", "d_type": "i1"}, {"label": "p9", "d_type": "i1"},
                            # DRUH PEVNÉ PŘEKÁŽKY (0-9), CHARAKTER NEHODY (1-2)
                            {"label": "p10", "d_type": "i1"}, {"label": "p11", "d_type": "i1"},
                            # ZAVINĚNÍ NEHODY (0-7), ALKOHOL U VINÍKA NEHODY PŘÍTOMEN (0-9)
                            {"label": "p12", "d_type": "i2"}, {"label": "p13a", "d_type": "i1"},
                            # HLAVNÍ PŘÍČINY NEHODY (100-615), USMRCENO OSOB (0-127)
                            {"label": "p13b", "d_type": "i1"}, {"label": "p13c", "d_type": "i1"},
                            # TĚŽCE ZRANĚNO OSOB (0-127), LEHCE ZRANĚNO OSOB(0-127)
                            {"label": "p14", "d_type": "i4"}, {"label": "p15", "d_type": "i1"},
                            # CELKOVÁ HMOTNÁ ŠKODA (0-2^31), DRUH POVRCHU VOZOVKY (1-6)
                            {"label": "p16", "d_type": "i1"}, {"label": "p17", "d_type": "i1"},
                            # STAV POVRCHU VOZOVKY V DOBĚ NEHODY (0-9), STAV KOMUNIKACE (1-12)
                            {"label": "p18", "d_type": "i1"}, {"label": "p19", "d_type": "i1"},
                            # POVĚTRNOSTNÍ PODMÍNKY V DOBĚ NEHODY (0-7), VIDITELNOST (1-7)
                            {"label": "p20", "d_type": "i1"}, {"label": "p21", "d_type": "i1"},
                            # ROZHLEDOVÉ POMĚRY (0-6), DĚLENÍ KOMUNIKACE (0-6)
                            {"label": "p22", "d_type": "i1"}, {"label": "p23", "d_type": "i1"},
                            # SITUOVÁNÍ NEHODY NA KOMUNIKACI (0-9), ŘÍZENÍ PROVOZU V DOBĚ NEHODY (0-3)
                            {"label": "p24", "d_type": "i1"}, {"label": "p27", "d_type": "i1"},
                            # MÍSTNÍ ÚPRAVA PŘEDNOSTI V JÍZDĚ (0-5), SPECIFICKÁ MÍSTA A OBJEKTY V MÍSTĚ NEHODY (0-10)
                            {"label": "p28", "d_type": "i1"}, {"label": "p34", "d_type": "i1"},
                            # SMĚROVÉ POMĚRY (1-7), POČET ZÚČASTNĚNÝCH VOZIDEL (0-127)
                            {"label": "p35", "d_type": "i1"}, {"label": "p39", "d_type": "i1"},
                            # MÍSTO DOPRAVNÍ NEHODY (0-29), DRUH KŘIŽUJÍCÍ KOMUNIKACE (1,2,3,6,7,9)
                            {"label": "p44", "d_type": "i1"}, {"label": "p45a", "d_type": "i1"},
                            # DRUH VOZIDLA (0-18), VÝROBNÍ ZNAČKA MOTOROVÉHO VOZIDLA(0-99)
                            {"label": "p47", "d_type": "i1"}, {"label": "p48a", "d_type": "i1"},
                            # ROK VÝROBY VOZIDLA (XX, 0-99), CHARAKTERISTIKA VOZIDLA (0-18)
                            {"label": "p49", "d_type": "i1"}, {"label": "p50a", "d_type": "i1"},
                            # SMYK (0-1), VOZIDLO PO NEHODĚ (0-4)
                            {"label": "p50b", "d_type": "i1"}, {"label": "p51", "d_type": "i1"},
                            # ÚNIK PROVOZNÍCH, PŘEPRAVOVANÝCH HMOT (0-4), ZPŮSOB VYPROŠTĚNÍ OSOB Z VOZIDLA (1-3)
                            {"label": "p52", "d_type": "i1"}, {"label": "p53", "d_type": "i4"},
                            # SMĚR JÍZDY NEBO POSTAVENÍ VOZIDLA (1 - 99), ŠKODA NA VOZIDLE v 100KČ (0 - 2^31)
                            {"label": "p55a", "d_type": "i1"}, {"label": "p57", "d_type": "i1"},
                            # KATEGORIE ŘIDIČE (0-9), STAV ŘIDIČE (0-9)
                            {"label": "p58", "d_type": "i1"},
                            # VNĚJŠÍ OVLIVNĚNÍ ŘIDIČE (0-5)
                            {"label": "a", "d_type": "f8"}, {"label": "b", "d_type": "f8"},
                            # LOKACE
                            {"label": "d", "d_type": "f8"}, {"label": "e", "d_type": "f8"},
                            # LOKACE
                            {"label": "f", "d_type": "f8"}, {"label": "g", "d_type": "f8"},
                            # LOKACE
                            {"label": "h", "d_type": "<U64"}, {"label": "i", "d_type": "<U32"},
                            # LOKACE txt
                            {"label": "j", "d_type": "i1"}, {"label": "k", "d_type": "<U32"},
                            #  NOT DEFINED IN DATASETS, TYP SILNICE txt
                            {"label": "l", "d_type": "<U6"}, {"label": "n", "d_type": "i8"},
                            # ČÍSLO SILNICE, XXX
                            {"label": "o", "d_type": "f8"}, {"label": "p", "d_type": "<U32"},
                            # XXX, SMĚR JÍZDY
                            {"label": "q", "d_type": "<U32"}, {"label": "r", "d_type": "i8"},
                            # PRUH, XXX
                            {"label": "s", "d_type": "i8"}, {"label": "t", "d_type": "<U32"},
                            # XXX, UNKNOWN IDETINTIFIER
                            {"label": "p5a", "d_type": "i1"},
                            # LOKALITA NEHODY (1-2)
                            {"label": "region", "d_type": "<U3"},
                            # REGION ('XXX')
                            ]

    @staticmethod
    def concat_np_data_list(prev_data, data_to_concat):
        """Concat two arrays of type list[np.ndarray]"""
        if prev_data is None:
            return data_to_concat
        for index, value in enumerate(data_to_concat):
            prev_data[index] = np.concatenate([prev_data[index], value])
        return prev_data

    @staticmethod
    def get_best_match(year, datasets):
        """Find the best dataset to avoid duplicity"""
        year_full_regex = re.compile(fr'.*[^\d\-]-?{year}.zip')
        highest_month = 0
        dataset_name = ''
        month_regex = re.compile(fr'.*(\d\d)-{year}.zip')
        for dataset in datasets:
            if year_full_regex.match(dataset):
                return dataset
            match = month_regex.search(dataset)
            if match:
                month = int(match.group(1))
                if month > highest_month:
                    highest_month = month
                    dataset_name = dataset
        if dataset_name == '':
            raise ValueError(f'Could not find valid dataset for year {year}')
        return dataset_name

    """Public methods"""

    def download_data(self):
        """Download all datasets available at specified url"""
        # regex to match only zip files
        re_name = re.compile(r'[^/]+\.zip')
        file_paths = self.__get_dataset_names_from_url()
        for file_path in file_paths:
            file_name = re_name.search(file_path).group(0)
            self.__download_file(file_path, file_name)

    def get_list(self, regions=None):
        """Get list of data corresponding to selected regions"""
        existing_regions = self.region_files
        if regions is None:
            regions = list(existing_regions.keys())
        if type(regions) != list:
            raise ValueError('Provided regions are not list')
        self.__actualize_datasets()
        non_parsed_regions = [region for region in regions if region not in self.parsed_regions]
        cached_regions, cached_region_files = self.__get_existing_cache_files()

        for region in non_parsed_regions:
            if region not in existing_regions:
                raise ValueError(f'Provided region {region} does not exist')
            # region is loaded in self.parsed_data
            if region in self.parsed_regions:
                continue
            if region in cached_regions:
                # we read content of cache file and store it to the self.parsed_data
                with open_gzip(path.join(self.folder, cached_region_files[cached_regions.index(region)]),
                               'rb') as cache_file:
                    data = pickle.load(cache_file)
                    self.__region_processed(region, data)
            else:
                # we need to parse region
                self.__process_region(region)
        # data are saved in dictionary so we need to get one list
        return self.__get_list_from_parsed_data(regions)

    def parse_region_data(self, region, should_actualize_datasets=True):
        """Parse data for current region to tuple(list[str], list[np.ndarray])"""
        if should_actualize_datasets:
            self.__download_missing_files()
        datasets = self.non_duplicate_datasets or self.__get_non_duplicate_datasets()
        parsed_data = [np.ndarray(shape=(0,), dtype=item['d_type']) for item in self.csv_headers]
        for dataset in datasets:
            with ZipFile(path.join(self.folder, dataset)) as archive:
                try:
                    with archive.open(self.region_files[region], 'r') as file:
                        parsed_data_to_merge = self.__parse_csv_file(file)
                        parsed_data_to_merge[-1][:] = region
                        parsed_data = DataDownloader.concat_np_data_list(parsed_data, parsed_data_to_merge)
                except KeyError:
                    raise KeyError(f'Provided region key: {region}, does not exist.')
        return [item['label'] for item in self.csv_headers], parsed_data

    """Private methods"""

    def __actualize_datasets(self):
        """Get datasets from url, if any new file is found clear cache files and corresponding class attributes"""
        # missing datasets available on provided url
        datasets_downloaded = self.__download_missing_files()
        if datasets_downloaded > 0:
            cache_regex = re.compile(self.cache_filename.format(r'(\w{3})'))
            # remove existing cache files
            files_in_directory = listdir(self.folder)
            for file in files_in_directory:
                match = cache_regex.match(file)
                if match:
                    remove_file(f'{self.folder}/{file}')
            # clear attributes
            self.parsed_data = {}
            self.parsed_regions = []
            self.non_duplicate_datasets = None

    def __download_file(self, file_path, file_name):
        """ Download data in stream mode for faster processing"""
        response = get_request(f'{self.url}{file_path}', headers=self.headers, stream=True)
        # we check for status code before setting data
        if response.status_code != 200:
            raise ConnectionError(f'Could not fetch {self.url}{file_path}')
        else:
            # write to output file in chunks for faster
            with open(f'{self.folder}/{file_name}', 'wb+') as file:
                for chunk in response.iter_content(chunk_size=128):
                    file.write(chunk)

    def __download_missing_files(self):
        """Detect any missing file in cwd"""
        # regex to match only zip files
        re_name = re.compile(r'[^/]+\.zip')
        # get all available dataset paths in table on specified url and theirs names
        file_paths = self.__get_dataset_names_from_url()
        # get all name of files in our cwd
        existing_file_names = self.__get_existing_datasets()
        files_downloaded = 0
        for item in file_paths:
            file_name = re_name.search(item).group(0)
            if file_name not in existing_file_names:
                files_downloaded += 1
                self.__download_file(item, file_name)
        return files_downloaded

    def __get_existing_cache_files(self):
        """Get all cache names and corresponding regions available in cwd"""
        files_in_directory = listdir(self.folder)
        cache_regex = re.compile(self.cache_filename.format(r'(\w{3})'))
        file_names = []
        regions = []
        for file in files_in_directory:
            match = cache_regex.match(file)
            if match:
                file_names.append(match.group(0))
                regions.append(match.group(1))
        return regions, file_names

    def __get_existing_datasets(self):
        """Get all dataset names available in cwd"""
        files_in_directory = listdir(self.folder)
        return [file for file in files_in_directory if file.endswith(".zip")]

    def __get_dataset_names_from_url(self):
        """Get all dataset paths on specified url"""
        response = get_request(self.url, headers=self.headers)
        if response.status_code != 200:
            raise ConnectionError(f'Could not fetch url "{self.url}"')

        # get html and parse all a tags with .zip files
        response_html = response.text
        soup = BeautifulSoup(response_html, "html.parser")
        valid_dataset_re = re.compile(r'(\d{4})\.zip')
        a_tags = soup.table.find_all('a', {'href': valid_dataset_re})
        dataset_paths = [a_tag.attrs["href"] for a_tag in a_tags]

        # get unique years and find best matching files
        years = set([valid_dataset_re.search(dataset).group(1) for dataset in dataset_paths])
        return [DataDownloader.get_best_match(year, dataset_paths) for year in years]

    def __get_list_from_parsed_data(self, regions):
        """Merge dictionary values to one output array"""
        if len(regions) > 0:
            labels, _ = self.parsed_data[regions[0]]
            values = None
            for region in regions:
                values = DataDownloader.concat_np_data_list(values, self.parsed_data[region][1])
            return labels, values
        return [], []

    def __get_non_duplicate_datasets(self):
        """Get non duplicate dataset names in cwd, to parse correct data"""
        datasets = self.__get_existing_datasets()
        years = set([re.search(r'(\d{4}).zip', dataset).group(1) for dataset in datasets])

        # set it to attribute to avoid redoing same piece of code
        self.non_duplicate_datasets = [DataDownloader.get_best_match(year, datasets) for year in years]
        return self.non_duplicate_datasets

    def __parse_csv_file(self, file):
        """Parse single csv file and return list with numpy arrays"""
        number_of_rows = sum(1 for _ in file)
        parsed_data = [np.empty(shape=number_of_rows, dtype=item['d_type']) for item in self.csv_headers]
        file.seek(0)
        csv_reader = reader(TextIOWrapper(file, "Windows-1250"), delimiter=';', quotechar='"')
        for index, row in enumerate(csv_reader):
            for index_val, value in enumerate(row):
                if self.csv_headers[index_val]['d_type'] == 'f8':
                    try:
                        parsed_data[index_val][index] = value.replace(',', '.')
                    except ValueError:
                        parsed_data[index_val][index] = np.nan
                else:
                    try:
                        parsed_data[index_val][index] = value
                    except ValueError:
                        parsed_data[index_val][index] = -1
        return parsed_data

    def __process_region(self, region):
        """Parse region, create cache file and copy data to attribute self.parsedData"""
        data = self.parse_region_data(region, should_actualize_datasets=False)
        with open_gzip(path.join(self.folder, self.cache_filename.format(region)), 'wb') as file:
            pickle.dump(data, file)
            self.__region_processed(region, data)

    def __region_processed(self, region, data):
        """Add data to attributes"""
        self.parsed_data[region] = data
        self.parsed_regions.append(region)


if __name__ == '__main__':
    print('Downloading necessary files...')
    downloader = DataDownloader()
    regions_to_parse = ['MSK', 'PHA', 'STC']
    print(f'Parsing data for regions: {regions_to_parse}...')
    data_as_list = downloader.get_list(regions_to_parse)
    print('Data successfully parsed')
    print(f'Found {len(data_as_list[1][0])} records')
    print(f'Headers are: {(data_as_list[0])}')
