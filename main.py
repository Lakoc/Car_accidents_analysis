from download import DataDownloader
from get_stat import plot_stat
import time

start = time.time()
data_source = DataDownloader().get_list()
end = time.time()
print(end - start)
plot_stat(data_source, show_figure=True)
