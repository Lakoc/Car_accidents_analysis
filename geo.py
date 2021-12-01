#!/usr/bin/python3.8
# coding=utf-8
import pandas as pd
import geopandas
import matplotlib.pyplot as plt
import contextily as ctx
import sklearn.cluster
import numpy as np


# muzeze pridat vlastni knihovny

def _save_show_fig(fig_location: str, show_figure: bool, fig: plt.Figure):
    """Show and save file according to provided params"""
    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


def make_geo(df: pd.DataFrame) -> geopandas.GeoDataFrame:
    """ Konvertovani dataframe do geopandas.GeoDataFrame se spravnym kodovani"""
    df_cleaned = df.dropna(subset=['d', 'e'])
    # create GeoDataFrame in S-JTSK
    return geopandas.GeoDataFrame(df_cleaned,
                                  geometry=geopandas.points_from_xy(df_cleaned["d"], df_cleaned["e"]),
                                  crs="EPSG:5514")


def plot_geo(gdf: geopandas.GeoDataFrame, fig_location: str = None,
             show_figure: bool = False):
    """ Vykresleni grafu s dvemi podgrafy podle lokality nehody """

    # filter by region and use CRS WGS84
    region = 'MSK'
    gdf_region = gdf[gdf['region'] == region].to_crs("epsg:3857")

    # filter by location
    gdf_region_municipality = gdf_region[gdf_region['p5a'] == 1]
    gdf_region_outside = gdf_region[gdf_region['p5a'] == 2]

    # set figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 10), sharex=True, sharey=True)

    # remove axis spines
    ax1.axis("off")
    ax2.axis("off")

    # set corresponding titles
    ax1.set_title("Nehody v Moravskoslezském kraji: v obci", fontsize=18, fontweight='bold')
    ax2.set_title("Nehody v Moravskoslezském kraji: mimo obec", fontsize=18, fontweight='bold')

    # plot data and change markers for better view
    gdf_region_municipality.plot(ax=ax1, markersize=3, color='r')
    gdf_region_outside.plot(ax=ax2, markersize=3, color='g')

    # add base maps
    ctx.add_basemap(ax1, crs=gdf_region_municipality.crs.to_string(), source=ctx.providers.Stamen.TonerLite)
    ctx.add_basemap(ax2, crs=gdf_region_municipality.crs.to_string(), source=ctx.providers.Stamen.TonerLite)

    # prettier figure
    fig.tight_layout()
    _save_show_fig(fig_location, show_figure, fig)


def plot_cluster(gdf: geopandas.GeoDataFrame, fig_location: str = None,
                 show_figure: bool = False):
    """ Vykresleni grafu s lokalitou vsech nehod v kraji shlukovanych do clusteru """
    # filter by region and use CRS WGS84
    region = 'MSK'
    gdf_region = gdf[gdf['region'] == region].to_crs("epsg:3857")

    # get coordinates and group to clusters using k-means
    coords = np.dstack([gdf_region.geometry.x, gdf_region.geometry.y]).reshape(-1, 2)
    clusters = sklearn.cluster.MiniBatchKMeans(n_clusters=30).fit(coords)

    # copy dataset and group by clusters
    gdf_region_clusters = gdf_region.copy()
    gdf_region_clusters['cluster'] = clusters.labels_
    gdf_center_coords = geopandas.GeoDataFrame(
        geometry=geopandas.points_from_xy(clusters.cluster_centers_[:, 0], clusters.cluster_centers_[:, 1]))

    # merge values to centred points
    gdf_region_counts = gdf_region_clusters.groupby(['cluster'])['region'].count()
    gdf_center_coords['count'] = gdf_region_counts

    # set figure
    fig, ax = plt.subplots(1, 1, figsize=(20, 15))

    # remove axis spines
    ax.axis("off")

    # set corresponding titles
    ax.set_title("Nehody v Moravskoslezském kraji", fontsize=30, fontweight='bold')

    # plot data and change markers for better view
    gdf_region.plot(ax=ax, markersize=1, color='k', alpha=0.5)
    gdf_center_coords.plot(ax=ax, markersize=gdf_center_coords["count"], column="count", alpha=0.7, legend=True)

    # add base maps
    ctx.add_basemap(ax, crs=gdf_region.crs.to_string(), source=ctx.providers.Stamen.TonerLite)

    # prettier figure
    fig.tight_layout()
    _save_show_fig(fig_location, show_figure, fig)


if __name__ == "__main__":
    # zde muzete delat libovolne modifikace
    gdf = make_geo(pd.read_pickle("accidents.pkl.gz"))
    plot_geo(gdf, "geo1.png", True)
    plot_cluster(gdf, "geo2.png", True)
