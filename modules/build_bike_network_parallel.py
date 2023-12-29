import geopandas as gpd
import pandas as pd
import numpy as np
import multiprocessing as mp
from shapely.geometry import LineString, Point
import modules.log_controller
import yaml
import modules.configuration
import os


def calc_slope_parallel(link_id):

    _df_edges = global_model_links.loc[link_id]
    num_edge_points = len(_df_edges.geometry.coords)

    # slope points
    _elev_pts = global_elev_dict[link_id]

    config = yaml.safe_load(
        open(os.path.join(modules.configuration.args.configs_dir, "config.yaml"))
    )
    if config["output_crs"]:
        crs = config["output_crs"]
    else:
        crs = config["input_crs"]

    # Generate two geodataframes; one for an initial point, the other for the subsequent point
    # Each row of the dataframes represents a unique pair of points (a segment) in order across the linestring
    _gdf_seg_start = gpd.GeoDataFrame(
        geometry=[
            Point(_df_edges.geometry.coords[i]) for i in range(num_edge_points - 1)
        ],
        crs=crs,
    )
    _gdf_seg_end = gpd.GeoDataFrame(
        geometry=[
            Point(_df_edges.geometry.coords[i]) for i in range(1, num_edge_points)
        ],
        crs=crs,
    )

    # Calculate XY distance between each point
    xy_dist = pd.DataFrame(_gdf_seg_start.distance(_gdf_seg_end), columns=["xy_dist"])

    # Calculate elevation difference (z) bewteen the points using raster points
    if None not in _elev_pts:

        # Calculate difference in elevation from initial point to each next point
        tot_elev_change = [
            _elev_pts[i] - _elev_pts[i - 1] for i in range(1, len(_elev_pts))
        ]

        # Convert elevation from meters to feet
        tot_elev_change = [config["elev_conversion"] * i for i in tot_elev_change]

        # these results are joined to the coordinate info
        xy_dist["tot_elev_change"] = tot_elev_change
        # Calculate upslope only
        xy_dist["elev_increase"] = xy_dist["tot_elev_change"].copy()
        xy_dist.loc[xy_dist["elev_increase"] <= 0, "elev_increase"] = 0

        # Calculate total elevation increase (upslope) for the entire edge
        edge_upslope = xy_dist["elev_increase"].sum()
        avg_upslope = edge_upslope / _df_edges.geometry.length

    else:
        avg_upslope = -1

    return avg_upslope


def init_pool(model_links, elev_dict):
    global global_model_links
    global_model_links = model_links
    global global_elev_dict
    global_elev_dict = elev_dict
