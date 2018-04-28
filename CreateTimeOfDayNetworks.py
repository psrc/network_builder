import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np
from shapely.geometry import LineString
from shapely.geometry import Point

class CreateTimeOfDayNetworks(object):

    def __init__(self, network_gdf, junctions_gdf, time_period, config):
        self.network_gdf = network_gdf
        self.junctions_gdf = junctions_gdf
        self.time_period = time_period
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
        self.hov_edges = self._get_hov_edges() 
        self.hov_junctions = self._get_hov_junctions() 
        self.hov_weave_edges = self._get_weave_edges()

    def _shift_edges(self, geometry, distance):
        coords = geometry.coords
        return LineString([(x[0] + distance, x[1] + distance) for x in coords])

    def _shift_junctions(self, geometry, distance):
        coord = geometry.coords
        return Point([(x[0] + distance, x[1] + distance) for x in coord])

    def _get_hov_edges(self):
        ij_field_name = 'IJLanesHOV' + self.time_period
        ji_field_name = 'JILanesHOV' + self.time_period
        hov_edges = self.network_gdf[(self.network_gdf[ij_field_name] > 0) | (self.network_gdf[ji_field_name] > 0)]
        shift_edges = hov_edges.geometry.apply(self._shift_edges, args=(self.config['hov_shift_dist'],))
        # update the the geometry column
        hov_edges.update(shift_edges)
        #hov_edges = _update_hov_ij_nodes(self, hov_edges)
        return hov_edges

    def _get_hov_junctions(self):
        keep_nodes = list(set(self.hov_edges['INode'].tolist() + self.hov_edges['JNode'].tolist()))
        hov_junctions = self.junctions_gdf[self.junctions_gdf['PSRCjunctI'].isin(keep_nodes)]
        shift_junctions = hov_junctions.geometry.apply(self._shift_junctions, args=(self.config['hov_shift_dist'],))
        hov_junctions.update(shift_junctions)
        hov_junctions['ScenarioNodeID'] = range(self.junctions_gdf.PSRCjunctI.max() + 1, self.junctions_gdf.PSRCjunctI.max() + len(hov_junctions) + 1)
        # update edge I & J nodes
        self._update_hov_ij_nodes(hov_junctions)
        return hov_junctions

    def _update_hov_ij_nodes(self, hov_junctions):
        # create a map between the old node and the new node id
        recode_dict = dict(zip(hov_junctions['PSRCjunctI'], hov_junctions['ScenarioNodeID']))
        # first set new to old
        self.hov_edges['NewINode'] = self.hov_edges['INode']
        self.hov_edges['NewJNode'] = self.hov_edges['JNode']
        # now recode
        self.hov_edges["NewINode"].replace(recode_dict, inplace=True)
        self.hov_edges["NewJNode"].replace(recode_dict, inplace=True)
        
    def _get_weave_edges(self):
        # start with HOV junctions
        weave_edges = self.hov_junctions[['PSRCjunctI','geometry', 'ScenarioNodeID']]
        
        # JNode is on the HOV end, INode is on the GP end
        weave_edges['NewJNode'] = weave_edges['ScenarioNodeID'] 
        weave_edges.drop('ScenarioNodeID', 1, inplace=True)
        # left join gives the GP counterpoint to the HOV node
        weave_edges = weave_edges.merge(self.junctions_gdf, on = 'PSRCjunctI', how = 'left')
        # create line geometry using the GP & HOV Junctions
        weave_edges['geometry'] = weave_edges.apply(self._create_edge, axis = 1)
        weave_edges.drop('geometry_y', 1, inplace=True)
        weave_edges.drop('geometry_x', 1, inplace=True)
        weave_edges['NewINode'] = weave_edges['ScenarioNodeID'] 
        weave_edges = gpd.GeoDataFrame(weave_edges)
        return weave_edges



        print 'done'
    def _create_edge(self, row):
        # want the from node to be from the gp edge
        coord_y = list(row['geometry_x'].coords)
        coord_x = list(row['geometry_y'].coords)
        return LineString(coord_x + coord_y)
