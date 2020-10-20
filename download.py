import requests
import re
from os import path, makedirs
from bs4 import BeautifulSoup


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

    def download_data(self):
        """Download all data detected on specified url"""
        response = requests.get(self.url, headers=self.headers)
        if response.status_code != 200:
            print(f'Could not fetch url "{self.url}"')
            exit(1)

        # get html and parse all a tags with .zip files
        response_html = response.text
        soup = BeautifulSoup(response_html, "html.parser")
        a_tags = soup.table.find_all('a', {'href': re.compile(r'\.zip')})
        re_name = re.compile(r'[^/]*\.zip')
        for a_tag in a_tags:
            # get file names
            file_path = a_tag.attrs["href"]
            file_name = re_name.search(file_path).group(0)
            # get data in stream for faster processing
            response = requests.get(f'{self.url}{file_path}', headers=self.headers, stream=True)
            # we check for status code before setting data
            if response.status_code != 200:
                print(f'Could not fetch {self.url}{file_path}')
            else:
                # write to output file in chunks for faster
                with open(f'{self.folder}/{file_name}', 'wb+') as fd:
                    for chunk in response.iter_content(chunk_size=128):
                        fd.write(chunk)
