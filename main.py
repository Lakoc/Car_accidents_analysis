from download import DataDownloader

import time

start = time.time()
downloader = DataDownloader()
downloader.get_list()
end = time.time()
print(end - start)
