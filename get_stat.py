import numpy as np
import matplotlib.pyplot as plt
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Module for processing parsed data.')
    parser.add_argument('--fig_location', type=str,
                        help='path where to save graph outputs')
    parser.add_argument('--show_figure', action="store_true",
                        help='enables plotting')
    args = parser.parse_args()


def get_year_region_stats(regions, years, years_unique):
    """Generate tuple (labels, values, max_value) for simple creating of graph"""
    stats = []
    for index, year in enumerate(years_unique):
        year_values_indexes = np.where(years == year)
        regions_on_year = regions[year_values_indexes]
        labels, counts = np.unique(regions_on_year, return_counts=True)
        stats.append((labels, counts, year))
    max_values = [max(statistic[1]) for statistic in stats]

    return stats, max(max_values)


def set_annotation_of_bars(ax, bars):
    """Set order and value to bars"""
    heights = [bar.get_height() for bar in bars]
    heights.sort(reverse=True)

    for rect in bars:
        height = rect.get_height()
        order = heights.index(height) + 1
        ax.annotate('{}'.format(order),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha='center', va='bottom')
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, -15),
                    color="white",
                    textcoords="offset points",
                    ha='center', va='bottom')


def set_axis_content(axes, index, statistic, max_val):
    """Generate 1 subplot -> histogram"""
    bars = axes[index].bar(statistic[0], statistic[1], color='C3')

    axes[index].grid(axis='y', color='black', linewidth=.5, alpha=.5)
    axes[index].spines["top"].set_visible(False)
    axes[index].spines["right"].set_visible(False)
    axes[index].tick_params(left=False, bottom=False)
    axes[index].set_ylim([0, max_val])
    axes[index].set_ylabel(r"$\it{Počet}$ $\it{nehod}$")
    axes[index].set_xlabel(r"$\it{Zkratka}$ $\it{kraje}$")
    axes[index].set_title(statistic[2], fontsize=20)

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
    fig, axes = plt.subplots(nrows=len(years_unique), ncols=1, figsize=(12, 16))
    fig.suptitle('Počet nehod v přislušných letech v českých krajích', fontsize=24, fontweight="bold")

    # generate stats and get max value, for same looking graphs
    stats, max_val = get_year_region_stats(regions, years, years_unique)

    # generate axes and add padding for good looking output
    for index, statistic in enumerate(stats):
        set_axis_content(axes, index, statistic, max_val)
    fig.tight_layout(pad=2)

    # plot or save fig if enabled
    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()
