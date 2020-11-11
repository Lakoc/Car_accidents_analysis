from download import DataDownloader
from get_stat import plot_stat
import time

x = DataDownloader()
start = time.time()
data_source = x.get_list()
plot_stat(data_source, show_figure=True)
end = time.time()
print(end - start)