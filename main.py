from download import DataDownloader

import time

downloader = DataDownloader()
start = time.time()
downloader.get_list()
end = time.time()
print(end - start)
