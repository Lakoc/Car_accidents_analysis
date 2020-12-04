#!/usr/bin/env python3.8
# coding=utf-8

from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import os
import re
# muzete pridat libovolnou zakladni knihovnu ci knihovnu predstavenou na prednaskach
# dalsi knihovny pak na dotaz


def _check_if_path_exist(filename):
    if not os.path.exists(filename):
        raise OSError(f'Could not find: {filename}')


def _print_dataframe_size(prefix: str, dataframe: pd.DataFrame):
    """Calculate size of provided dataframe and print it's size in MB"""
    size = dataframe.memory_usage(deep=True).sum()
    print(f'{prefix}={size / 1048576:.1f} MB')


def _optimize_dataframe_size(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Add datetime64 to provided dataframe by reading column 'p2a', remove unnecessary values and categorize columns"""
    columns_to_drop = ['p2a', 'weekday(p2a)']
    # columns which after categorization smaller dataframe size
    columns_to_categorize = ['p36', 'p37', 'p2b', 'p6', 'p7', 'p8', 'p9', 'p10', 'p11', 'p12', 'p13a', 'p13b', 'p13c',
                             'p14', 'p15', 'p16', 'p17', 'p18', 'p19', 'p20', 'p21', 'p22', 'p23', 'p24', 'p27', 'p28',
                             'p34', 'p35', 'p39', 'p44', 'p45a', 'p47', 'p48a', 'p49', 'p50a', 'p50b', 'p51', 'p52',
                             'p53', 'p55a', 'p57', 'p58', 'h', 'i', 'j', 'k', 'l', 'n', 'o', 'p', 'q', 'r', 's', 't',
                             'p5a', 'date']
    # add date to dataframe
    dates = pd.to_datetime(dataframe['p2a'])
    dataframe['date'] = dates

    # remove unnecessary columns
    dataframe = dataframe.drop(columns=columns_to_drop)

    # categorize provided columns
    for col in columns_to_categorize:
        dataframe[col] = dataframe[col].astype('category')
    return dataframe


# Ukol 1: nacteni dat
def get_dataframe(filename: str = "accidents.pkl.gz", verbose: bool = False) -> pd.DataFrame:
    """Read dataframe from provided file, optimize size and in verbose mode print old and new size"""

    # provided filename does not exist, we raise os error
    _check_if_path_exist(filename)

    # load dataframe from pickle with auto decompression
    dataframe = pd.read_pickle(filename)

    if verbose:
        _print_dataframe_size('orig_size', dataframe)

    # optimize dataset size
    dataframe = _optimize_dataframe_size(dataframe)

    if verbose:
        _print_dataframe_size('new_size', dataframe)
    return dataframe


def _set_axis_content(ax: plt.axis, data: pd.DataFrame, label: str):
    sns.barplot(x=data.index, y=data.value, data=data, ax=ax, palette="flare",
                order=data.sort_values('value', ascending=False).index)
    ax.tick_params(bottom=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylabel(r"$\it{Počet}$", fontsize=8)
    ax.set_xlabel(r"$\it{Region}$", fontsize=8)
    ax.set_title(label, fontsize=12)

    for index, (_, row) in enumerate(data.sort_values('value', ascending=False).iterrows()):
        ax.text(index, row.value, row.value, color='black', ha="center")


def _save_show_fig(fig_location: str, show_figure: bool, fig: plt.Figure):
    if fig_location:
        folder = re.match(r'(.+/).+', fig_location)
        if folder:
            folder = folder.group(1)
            if not os.path.exists(folder):
                try:
                    os.makedirs(folder)
                except OSError:
                    raise OSError(f'Could not create directory: {folder}')
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


# Ukol 2: následky nehod v jednotlivých regionech
def plot_conseq(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    columns = ['p13a', 'p13b', 'p13c']
    labels = {'p13a': 'Počet umrtí', 'p13b': 'Počet těžkých zranění', 'p13c': 'Počet lehkých zranění',
              'all': 'Celkem nehod'}
    df_melted = pd.melt(df, id_vars=['region'], value_vars=columns)
    df_groups = df_melted.groupby(['variable', 'region']).sum()
    accidents_count = df['region'].value_counts()

    # Set up the matplotlib figure
    fig, axes = plt.subplots(4, 1, figsize=(9, 9))
    for index, column in enumerate(columns):
        data = df_groups.query(f'variable == "{column}"').droplevel('variable')
        _set_axis_content(axes[index], data, labels[column])
    _set_axis_content(axes[3], accidents_count.to_frame().rename(columns={'region': 'value'}), labels['all'])
    fig.suptitle('Nehody v regionech ČR', fontsize=20, fontweight="bold")
    fig.tight_layout()
    _save_show_fig(fig_location, show_figure, fig)


def _clean_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df[(df['p12'] == 100) | ((df['p12'] >= 201) & (df['p12'] <= 209)) | (
            (df['p12'] >= 301) & (df['p12'] <= 311)) | (
                    (df['p12'] >= 401) & (df['p12'] <= 414)) | (
                    (df['p12'] >= 501) & (df['p12'] <= 516)) | (
                    (df['p12'] >= 601) & (df['p12'] <= 615))]
    return df[df['p53'] >= 0]


def _plot_sub_damage(ax: plt.axis, data: pd.DataFrame, label: str):
    sns.barplot(x='p53', y='size', hue='p12', data=data, ax=ax)
    ax.set_yscale("log")
    ax.legend([], [], frameon=False)


# Ukol3: příčina nehody a škoda
def plot_damage(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    """TODO"""
    regions = ['JHM', 'HKK', 'PLK', 'PHA']
    labels_cause = ['nezaviněná řidičem', 'nepřiměřená rychlost jízdy', 'nesprávné předjíždění',
                    'nedání přednosti v jízdě',
                    'nesprávný způsob jízdy', 'technická závada vozidla']
    labels_damage = ['<50', '50 - 200', '200 - 500', '500 - 1000', '>1000']
    columns = ['region', 'p53', 'p12']
    # select only interesting columns
    df_regions = df[df['region'].isin(regions)][columns]

    df_regions['p12'] = df_regions['p12'].astype('int')
    df_regions['p53'] = df_regions['p53'].astype('int')

    # clean data from random values, if necessary
    df_regions = _clean_values(df_regions)

    df_regions['p12'] = pd.cut(df_regions['p12'], [99, 199, 299, 399, 499, 599, 699], labels=labels_cause)
    df_regions['p53'] = pd.cut(df_regions['p53'], bins=[-np.inf, 50, 200, 500, 1000, np.inf],
                               labels=labels_damage)
    df_regions = df_regions.groupby(columns, as_index=False).size()

    fig, axes = plt.subplots(2, 2, figsize=(9, 9))
    for index, region in enumerate(regions):
        ax = axes.flatten()[index]
        data = df_regions[df_regions['region'] == region]
        _plot_sub_damage(ax, data, region)

    handles, labels = axes[0][0].get_legend_handles_labels()
    fig.legend(handles, labels, loc='center right')

    fig.suptitle('Nehody v regionech ČR', fontsize=20, fontweight="bold")
    fig.tight_layout()
    _save_show_fig(fig_location, show_figure, fig)


# Ukol 4: povrch vozovky
def plot_surface(df: pd.DataFrame, fig_location: str = None,
                 show_figure: bool = False):
    pass


if __name__ == "__main__":
    pass
    # zde je ukazka pouziti, tuto cast muzete modifikovat podle libosti
    # skript nebude pri testovani pousten primo, ale budou volany konkreni ¨
    # funkce.
    df = get_dataframe("accidents.pkl.gz")
    # plot_conseq(df, fig_location="01_nasledky.png", show_figure=True)
    plot_damage(df, "02_priciny.png", True)
    plot_surface(df, "03_stav.png", True)
