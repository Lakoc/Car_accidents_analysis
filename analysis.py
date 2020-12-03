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
from gzip import open as open_gzip
import pickle


def _check_if_path_exist(filename):
    if not os.path.exists(filename):
        raise OSError(f'Could not find: {filename}')


def _save_show_fig(fig_location: str, show_figure: bool, fig: sns.FacetGrid):
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
        _print_dataframe_size('', dataframe)
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


# Ukol 2: následky nehod v jednotlivých regionech
def plot_conseq(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    pass


# Ukol3: příčina nehody a škoda
def plot_damage(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    pass


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
    plot_conseq(df, fig_location="01_nasledky.png", show_figure=True)
    plot_damage(df, "02_priciny.png", True)
    plot_surface(df, "03_stav.png", True)
