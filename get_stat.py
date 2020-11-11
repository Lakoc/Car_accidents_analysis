import numpy as np

from matplotlib.pyplot import subplots
from argparse import ArgumentParser
from re import match
from os import path, makedirs
from download import DataDownloader


def set_annotation_of_bars(ax, bars):
    """Set order and value to bars"""
    heights = [bar.get_height() for bar in bars]
    heights.sort(reverse=True)

    for rect in bars:
        height = rect.get_height()
        order = heights.index(height) + 1
        ax.annotate('{}'.format(order),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 0),
                    textcoords="offset points",
                    ha='center', va='bottom')
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, -12),
                    color="white",
                    textcoords="offset points",
                    ha='center', va='bottom')


def set_axis_content(axes, index, year, statistic):
    """Generate 1 subplot -> histogram"""
    bars = axes[index].bar(statistic[0], statistic[1], color='C3')

    axes[index].grid(axis='y', color='black', linewidth=.5, alpha=.5)
    axes[index].spines["top"].set_visible(False)
    axes[index].spines["right"].set_visible(False)
    axes[index].tick_params(left=False, bottom=False)
    axes[index].set_ylabel(r"$\it{Počet}$ $\it{nehod}$")
    axes[index].set_xlabel(r"$\it{Zkratka}$ $\it{kraje}$")
    axes[index].set_title(year, fontsize=20)

    set_annotation_of_bars(axes[index], bars)


def plot_stat(data_source, fig_location=None, show_figure=False):
    """Generate histogram about count of accidents in regions by years"""
    labels, data = data_source

    # get only interesting values from parsed data
    dates = data[labels.index('p2a')]
    regions = data[labels.index('region')]

    # exclude years from datetime and get unique years
    years = dates.astype('datetime64[Y]').astype("u2") + 1970
    years_unique = np.unique(years)
    years_unique = years_unique[np.where(years_unique > 2015)]

    # create figure with axes corresponding with unique years
    fig, axes = subplots(nrows=len(years_unique), ncols=1, figsize=(12, 16), sharey=True)
    fig.suptitle('Počet nehod v přislušných letech v českých krajích', fontsize=24, fontweight="bold")

    # generate axes and add padding for good looking output
    for index, year in enumerate(years_unique):
        year_statistic = np.unique(regions[np.where(years == year)], return_counts=True)
        set_axis_content(axes, index, year, year_statistic)
    fig.tight_layout(pad=2)

    # plot or save fig if enabled
    if fig_location:
        folder = match(r'(.+/).+', fig_location)
        if folder:
            folder = folder.group(1)
            if not path.exists(folder):
                try:
                    makedirs(folder)
                except OSError:
                    raise OSError(f'Could not create directory: {folder}')
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


if __name__ == '__main__':
    parser = ArgumentParser(description='Module for processing parsed data.')
    parser.add_argument('--fig_location', type=str,
                        help='path where to save graph outputs')
    parser.add_argument('--show_figure', action="store_true",
                        help='enables plotting')
    args = parser.parse_args()
    regions_to_parse = ['HKK', 'JHC', 'JHM', 'KVK', 'LBK', 'MSK', 'OLK', 'PAK', 'PHA', 'PLK', 'STC', 'ULK', 'VYS',
                        'ZLK']
    print(f'Parsing data for regions: {regions_to_parse}...')
    parsed_data = DataDownloader().get_list(regions_to_parse)
    print(f'Data were successfully parsed.')
    plot_stat(parsed_data, args.fig_location, args.show_figure)
