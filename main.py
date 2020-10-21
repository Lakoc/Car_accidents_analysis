from download import DataDownloader

import time
start = time.time()
downloader = DataDownloader()
downloader.parse_region_data("PHA")
end = time.time()
print(end - start)
# downloader.get_list()