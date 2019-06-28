import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
from rasterstats import zonal_stats, point_query
from shapely.geometry import LineString, Point
import log_controller

class BuildBikeNetwork(object):
    def __init__(self, scenario_edges, config):
        self.network_gdf = scenario_edges
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
        self.bike_network = self.create_bike_network()

    def calc_edge_upslope(self, df_edges, pts, index_val, crs):
        """
        Calculate directional increase in slope along a linestring. 

        df_edges: a LINESTRING geodataframe (a line/edge feature made up of any number of edges)
        pts: intersect of elevation raster and the df_edges file; elevation unit should be consistent between feet and meters
        index_val: index of edge and pts intersect

        returns DataFrame of cumulative increased elevation between points along a line. Downward slope between points is considered zero upslope.
        """

        _df_edges = df_edges.iloc[index_val]
        num_edge_points = len(df_edges.iloc[index_val].geometry.coords)

        # Generate two geodataframes; one for an initial point, the other for the subsequent point
        # Each row of the dataframes represents a unique pair of points (a segment) in order across the linestring
        _gdf_seg_start = gpd.GeoDataFrame(geometry=[Point(_df_edges.geometry.coords[i]) for i in xrange(num_edge_points-1)], crs=crs)
        _gdf_seg_end = gpd.GeoDataFrame(geometry=[Point(_df_edges.geometry.coords[i]) for i in xrange(1, num_edge_points)], crs=crs)

        # Calculate XY distance between each point
        xy_dist = pd.DataFrame(_gdf_seg_start.distance(_gdf_seg_end), columns=['xy_dist'])

        # Calculate elevation difference (z) bewteen the points using raster points
        if None not in pts[index_val]:
            _elev_pts = pts[index_val]

            # Calculate difference in elevation from initial point to each next point
            tot_elev_change = [_elev_pts[i] - _elev_pts[i-1] for i in xrange(1,len(_elev_pts))]

            # Convert elevation from meters to feet
            tot_elev_change = [i*self.config['elev_conversion'] for i in tot_elev_change]

            # these results are joined to the coordinate info
            xy_dist['tot_elev_change'] = tot_elev_change
            # Calculate upslope only
            xy_dist['elev_increase'] = xy_dist['tot_elev_change'].copy()
            xy_dist.loc[xy_dist['elev_increase'] <= 0, 'elev_increase'] = 0

            # Calculate total elevation increase (upslope) for the entire edge
            edge_upslope = xy_dist['elev_increase'].sum()

        else:
            edge_upslope = -1

        return edge_upslope

    def create_bike_network(self):

        df_edges = self.network_gdf

        # Get raster elevation height, store as an array
        # What unit is the raster in? Assuming meters
        print 'extracting point elevation from raster'
        pts = np.array(point_query(df_edges, self.config['raster_file_path']))

        # Set coordinate reference system for spatial analysis
        crs =  {'init' : self.config['crs']['init']}

        results = []
        for i in xrange(len(df_edges)):
            if i % 1000 == 0:
                print("%d Edges Processed" % (i))
            results.append(self.calc_edge_upslope(df_edges=df_edges, pts=pts, index_val=i, crs=crs))

        df_edges['tot_upslope'] = results

        # Filter out missing values??

        # Compute the "average upslope" (average grade) across the length of the edge
        df_edges['length'] = df_edges.geometry.length
        df_edges['avg_upslope'] = df_edges['tot_upslope']/df_edges['length']

        # Some of these links are actually bridges that do not have as high of slopes as suggested
        # We can give all bridges 0 slope, though that's not always true
        # Get bridge field from OSM?

        #df_edges_filtered = df_edges[df_edges['avg_upslope'] > 0]

        #df_edges[df_edges['PSRCEdgeID'] == 182105][['tot_upslope']]

        return df_edges

