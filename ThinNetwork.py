import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString
import log_controller
import networkx as nx
from networkx.algorithms.components import *
import copy
import numpy as np

class ThinNetwork(object):
    def __init__(self, network_gdf, junctions_gdf, thin_nodes_list, config):
        self.network_gdf = network_gdf
        self.junctions_gdf = junctions_gdf
        self.thin_nodes_list = thin_nodes_list
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
        self.thinned_edges_gdf = self._thin_network()
        self.thinned_junctions_gdf = self._thin_junctions()
    
    def _get_sub_net(self):
            return self.network_gdf[self.network_gdf.IJ.isin (self.thin_nodes_list) | self.network_gdf.IJ.isin (self.thin_nodes_list)]

    def _compare_attributes(self, edge1, edge2, edge2_dir= 'IJ'):
        compare_cols = self.config['non_dir_columns'] + self.config['dir_columns']
        compare_atts1 = {key: value for (key, value) in edge1.iteritems() if key in compare_cols}
        
        if edge2_dir =='IJ':
            compare_atts2 = {key: value for (key, value) in edge2.iteritems() if key in compare_cols}
            if compare_atts1 == compare_atts2:
                return True
            else:
                return False
        elif edge2_dir =='JI': 
            compare_atts2 = {key[1] + key[0] + key[2:] : value for (key, value) in edge2.iteritems() if key in self.config['dir_columns']}
            compare_atts2.update({key: value for (key, value) in edge2.iteritems() if key in self.config['non_dir_columns']})
            if compare_atts1 == compare_atts2:
                return True
            else:
                return False
        else:
            return False

    def _thin_network(self):
        cols = self.config['intermediate_keep_columns'] + self.config['dir_columns']
        G = nx.from_pandas_edgelist(self.network_gdf, 'INode', 'JNode', cols)
        i = 0
        for node in self.thin_nodes_list:
            if i % 1000 == 0:
                print("%d Nodes Processed" % (i))
            edges = list(G.edges(node))
            check_edges = self._check_edge_connection_validity(node, edges, G)
            if check_edges:
                edge_1 = check_edges[0]
                edge_2 = check_edges[1]
                a_coords = list(edge_1['geometry'].coords)
                b_coords = list(edge_2['geometry'].coords)
                # get the first and last coord for the two edges
                a_test = [a_coords[0], a_coords[-1]]
                b_test = [b_coords[0], b_coords[-1]]
                if edge_1['INode'] <> edge_2['INode'] and edge_1['JNode'] <> edge_2['JNode']:
                    edge_dir = 'with'
                    merge = self._compare_attributes(edge_1, edge_2, 'IJ')
                else:
                    edge_dir = 'against'
                    merge = self._compare_attributes(edge_1, edge_2, 'JI')
                if merge:
                    if edge_dir == 'with':
                        # Do the first coords match or the first and last 
                        if a_test.index(list(set(a_test).intersection(b_test))[0]) == 0 :
                            order = 'ba'
                            a_coords.pop(0)
                            x = b_coords + a_coords 
                            line = LineString(x)
                            merged_row = edge_2
                            merged_row['geometry'] = line 
                            merged_row['JNode'] = edge_1['JNode']
                            G.remove_edge(edges[0][0], edges[0][1])
                            G.remove_edge(edges[1][0], edges[1][1])
                            G.add_edge(merged_row['INode'], merged_row['JNode'], **merged_row)
                        else:
                            order = 'ab'
                            b_coords.pop(0)
                            x = a_coords + b_coords 
                            line = LineString(x)
                            merged_row = edge_1
                            merged_row['geometry'] = line 
                            merged_row['JNode'] = edge_2['JNode']
                            G.remove_edge(edges[0][0], edges[0][1])
                            G.remove_edge(edges[1][0], edges[1][1])
                            G.add_edge(merged_row['INode'], merged_row['JNode'], **merged_row)
                      
        
                    # Are lines digitized towards each other:
                    elif edge_1['JNode'] == edge_2['JNode']:
                        #Flip the b line
                        b_coords.reverse()
                        # drop the duplicate coord
                        b_coords.pop(0)
                        x = a_coords + b_coords
                        line = LineString(x)
                        merged_row = edge_1
                        merged_row['geometry'] = line 
                        merged_row['INode'] = edge_1['INode']
                        merged_row['JNode'] = edge_2['INode']
                        G.remove_edge(edges[0][0], edges[0][1])
                        G.remove_edge(edges[1][0], edges[1][1])
                        G.add_edge(merged_row['INode'], merged_row['JNode'], **merged_row)                
                        

                    # Lines must be digitized away from each other:
                    else:
                        # drop the duplicate coord
                        b_coords.pop(0)
                        #Flip the b line
                        b_coords.reverse()
                        x = b_coords + a_coords
                        line = LineString(x)
                        merged_row = edge_1
                        merged_row['geometry'] = line 
                        merged_row['INode'] = edge_2['JNode']
                        merged_row['JNode'] = edge_1['JNode']
                        G.remove_edge(edges[0][0], edges[0][1])
                        G.remove_edge(edges[1][0], edges[1][1])
                        G.add_edge(merged_row['INode'], merged_row['JNode'], **merged_row)
            i = i + 1

        edge_list = []
        for x in G.edges.iteritems():
            edge_list.append(x[1])
        thinned_net = gpd.GeoDataFrame(edge_list)
        return thinned_net

    def _thin_junctions(self):
        keep_nodes = list(set(self.thinned_edges_gdf['INode'].tolist() + self.thinned_edges_gdf['JNode'].tolist()))
        thinned_junctions = self.junctions_gdf[self.junctions_gdf['PSRCjunctI'].isin(keep_nodes)]
        thinned_junctions['ScenarioNodeID'] = thinned_junctions['PSRCjunctI'] + self.config['node_offset']
        thinned_junctions['ScenarioNodeID'] = np.where(thinned_junctions['EMME2nodeI'] > 0, thinned_junctions['EMME2nodeI'], thinned_junctions['ScenarioNodeID'])
        # now make a map of old to new
        recode_dict = pd.Series(thinned_junctions.ScenarioNodeID.values, thinned_junctions.PSRCjunctI.values).to_dict()
        #recode_edges
        self.thinned_edges_gdf['NewINode'] = self.thinned_edges_gdf['INode']
        self.thinned_edges_gdf['NewJNode'] = self.thinned_edges_gdf['JNode']
        self.thinned_edges_gdf['NewINode'].replace(recode_dict, inplace=True)
        self.thinned_edges_gdf['NewJNode'].replace(recode_dict, inplace=True)
        return thinned_junctions

    def _check_edge_connection_validity(self, node, edges, network_graph):
        if len(edges) <> 2:
            self._logger.warning('WARNING: Node ' + str(node) + ' does not have exactly two edges!')
            return None
        else:
            edge_1 = network_graph.get_edge_data(edges[0][0], edges[0][1])
            edge_2 = network_graph.get_edge_data(edges[1][0], edges[1][1])
            if edge_1['geometry'].type =='MultiLineString':
                self._logger.warning('WARNING: edge ' + str(edge_1['PSRCEdgeID']) + ' is a multipart feature. Please fix.')  
                return None
            elif edge_2['geometry'].type =='MultiLineString':
                self._logger.warning('WARNING: edge ' + str(edge_2['PSRCEdgeID']) + ' is a multipart feature. Please fix.')  
                return None
            else:
                a_coords = list(edge_1['geometry'].coords)
                b_coords = list(edge_2['geometry'].coords)
                # get the first and last coord for the two edges
                a_test = [a_coords[0], a_coords[-1]]
                b_test = [b_coords[0], b_coords[-1]]
                if len(list(set(a_test).intersection(b_test))) == 0:
                    self._logger.info('WARNING: ' + str(edge_1['PSRCEdgeID']) + " " +  str(edge_2['PSRCEdgeID']) + " are not connected!")
                    return None
                else:
                    return (edge_1, edge_2)