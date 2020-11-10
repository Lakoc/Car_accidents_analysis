from download import DataDownloader
from get_stat import plot_stat
import time

x = DataDownloader()
start = time.time()
data_source = x.parse_region_data("JHM")
end = time.time()
print(end - start)