import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np
from shapely.geometry import LineString
from shapely.geometry import Point


class BuildHOVSystem(object):

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
        '''
        Shifts the location of an edge by
        an specified distance. Used to create
        distcinct HOV/Managed lanes parallel to
        GP.
        '''
        coords = geometry.coords
        return LineString([(x[0] + distance, x[1] + distance) for x in coords])

    def _shift_junctions(self, geometry, distance):
        '''
        Shifts the location of a jucntion by
        a specified amount.
        '''
        coord = geometry.coords
        return Point([(x[0] + distance, x[1] + distance) for x in coord])

    def _get_hov_edges(self):
        '''
        Returns a DF of shifted edges that represent
        HOV/Managed lanes.
        '''
        ij_field_name = 'IJLanesHOV' + self.time_period
        ji_field_name = 'JILanesHOV' + self.time_period
        # Get edges that have an hov attribute for this time period
        hov_edges = self.network_gdf[(self.network_gdf[ij_field_name] >
                                      0) | (self.network_gdf
                                            [ji_field_name] > 0)]
        # Shift them
        shift_edges_geom = hov_edges.geometry.apply(
            self._shift_edges, args=(self.config['hov_shift_dist'],))

        # Update the the geometry column
        hov_edges.update(shift_edges_geom)
        # Hov_edges = _update_hov_ij_nodes(self, hov_edges)
        hov_edges['FacilityType'] = 999
        return hov_edges

    def _get_hov_junctions(self):
        '''
        Returns a DF of shifted junctions that represent
        the end points of HOV/Managed lane.
        '''

        keep_nodes = list(set(self.hov_edges['INode'].tolist() +
                              self.hov_edges['JNode'].tolist()))

        hov_junctions = self.junctions_gdf[self.junctions_gdf['PSRCjunctID'].
                                           isin(keep_nodes)]

        shift_junctions = hov_junctions.geometry.apply(
            self._shift_junctions, args=(self.config['hov_shift_dist'],))

        hov_junctions.update(shift_junctions)
        hov_junctions['ScenarioNodeID'] = range(
            self.junctions_gdf.ScenarioNodeID.max().astype(int) + 1,
            self.junctions_gdf.ScenarioNodeID.max().astype(int) + len(hov_junctions) + 1)

        # Update edge I & J nodes
        self._update_hov_ij_nodes(hov_junctions)
        return hov_junctions

    def _update_hov_ij_nodes(self, hov_junctions):
        '''
        Updates the I and J attritubtes of the
        new HOV edges with the id of the node/junction
        at their end points.
        '''
        recode_dict = dict(zip(hov_junctions['PSRCjunctID'],
                               hov_junctions['ScenarioNodeID']))

        # First set new to old
        self.hov_edges['NewINode'] = self.hov_edges['INode']
        self.hov_edges['NewJNode'] = self.hov_edges['JNode']
        # now recode
        self.hov_edges["NewINode"].replace(recode_dict, inplace=True)
        self.hov_edges["NewJNode"].replace(recode_dict, inplace=True)

    def _get_weave_edges(self):
        '''
        Creates the edges taht connect the
        exisitng GP nodes to the new HOV nodes,
        which allows movements between the GP
        and Manged lane systesm.
        '''
        # Start with HOV junctions
        weave_edges = self.hov_junctions[['PSRCjunctID',
                                          'geometry', 'ScenarioNodeID']]

        # JNode is on the HOV end, INode is on the GP end
        weave_edges['NewJNode'] = weave_edges['ScenarioNodeID']
        weave_edges.drop('ScenarioNodeID', 1, inplace=True)
        # Left join gives the GP counterpoint to the HOV node
        weave_edges = weave_edges.merge(self.junctions_gdf,
                                        on='PSRCjunctID', how='left')

        # Create line geometry using the GP & HOV Junctions
        weave_edges['geometry'] = weave_edges.apply(
            self._create_edge, axis=1)
        weave_edges.drop('geometry_y', 1, inplace=True)
        weave_edges.drop('geometry_x', 1, inplace=True)
        weave_edges['NewINode'] = weave_edges['ScenarioNodeID']
        weave_edges['FacilityType'] = 98
        weave_edges['Oneway'] = 2
        weave_edges = gpd.GeoDataFrame(weave_edges)
        return weave_edges

    def _create_edge(self, row):
        '''
        Creates a line from one point
        to another. Used to create weave
        links/connectors.
        '''
        # Want the from node to be from the gp edge
        coord_y = list(row['geometry_x'].coords)
        coord_x = list(row['geometry_y'].coords)
        return LineString(coord_x + coord_y)
