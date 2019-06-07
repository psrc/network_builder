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

    def _compare_attributes(self, edge1, edge2, edge2_dir='IJ'):
        '''
        Boolean attribute comparison of two network edges.
        '''

        compare_cols = self.config['non_dir_columns'
                                   ] + self.config['dir_columns'] + self.config['dir_toll_columns']

        compare_atts1 = {key: value for (key, value) in edge1.iteritems()
                         if key in compare_cols}

        if edge2_dir == 'IJ':
            compare_atts2 = {key: value for (key, value) in edge2.iteritems()
                             if key in compare_cols}

            if compare_atts1 == compare_atts2:
                return True
            else:
                return False

        elif edge2_dir == 'JI':
            compare_atts2 = {key[1] + key[0] + key[2:]: value for
                             (key, value) in edge2.iteritems() if
                             key in self.config['dir_columns'] + self.config['dir_toll_columns']}

            compare_atts2.update({key: value for (key, value) in
                                  edge2.iteritems() if key in
                                  self.config['non_dir_columns']})
            
            if compare_atts1 == compare_atts2:
                return True
            else:
                return False

        else:
            return False

    def _report_duplicate_edges(self):
        '''
        Reports a warning in the log if
        there are duplicate edges (same i
        and j nodes).
        '''

        reverse_net = self.network_gdf.loc[self.network_gdf['Oneway'] == 2]
        reverse_net = reverse_net.rename(
            columns={'INode': 'JNode', 'JNode': 'INode'})

        full_net = pd.concat([self.network_gdf, reverse_net])
        full_net['id'] = full_net.INode.astype(
            str) + '-' + full_net.JNode.astype(str)

        full_net['freq'] = full_net.groupby(
            'id')['id'].transform('count')

        dup_edges = full_net.loc[full_net.freq > 1]
        dup_edges_dict = dup_edges.groupby(['id']).apply(
            lambda x: list(x.PSRCEdgeID)).to_dict()

        for node_seq, edge_ids in dup_edges_dict.iteritems():
            self._logger.info(
                 'Warning! Node sequence %s is represented '
                 'by more than one edge: %s. Please Fix!'
                 % (node_seq, edge_ids))

    def _thin_network(self):
        '''
        Returns a network with elgible edges
        merged).
        '''

        self._report_duplicate_edges()
        cols = self.config['intermediate_keep_columns'
                           ] + self.config['dir_columns'] + self.config['dir_toll_columns']
        
        # need to remove any links that are one-way, 
        # but share the reverse node sequence
        # these are merged back in after thinning. 
        thin_edges = self.network_gdf.copy()
        merge_edges = self.network_gdf.copy()[['INode', 'JNode']]
        merge_edges = merge_edges.rename(columns = {'INode' : 'INode_y', 'JNode' : 'JNode_y'})
        one_way_keep = thin_edges.merge(merge_edges, how = 'inner', left_on =  ['INode', 'JNode'], right_on = ['JNode_y', 'INode_y'])
        thin_edges = thin_edges[~thin_edges['PSRCEdgeID'].isin(one_way_keep['PSRCEdgeID'].tolist())]
        
        G = nx.from_pandas_edgelist(thin_edges,
                                    'INode', 'JNode', cols)

        i = 0
        node_list = [x for x in self.thin_nodes_list if G.has_node(x)]
        for node in node_list:
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

                if edge_1['INode'] != edge_2['INode'] and edge_1['JNode'] != edge_2['JNode']:

                    edge_dir = 'with'
                    merge = self._compare_attributes(edge_1, edge_2, 'IJ')

                else:
                    edge_dir = 'against'
                    merge = self._compare_attributes(edge_1, edge_2, 'JI')

                if merge:
                    if edge_dir == 'with':
                        # Do the first coords match or the first and last
                        if a_test.index(list(set(a_test)
                                             .intersection(b_test))[0]) == 0:
                            order = 'ba'
                            a_coords.pop(0)
                            x = b_coords + a_coords
                            line = LineString(x)
                            merged_row = edge_2
                            merged_row['geometry'] = line
                            merged_row['JNode'] = edge_1['JNode']
                            if G.has_edge(merged_row['INode'], merged_row['JNode']):
                                compare_edge = G.get_edge_data(merged_row['INode'],merged_row['JNode'])
                                if list(compare_edge['geometry'].coords) == x:
                                    print 'True'
                                    G.remove_edge(edges[0][0], edges[0][1])
                                    G.remove_edge(edges[1][0], edges[1][1])
                            else:
                                G.remove_edge(edges[0][0], edges[0][1])
                                G.remove_edge(edges[1][0], edges[1][1])
                                G.add_edge(merged_row['INode'],
                                       merged_row['JNode'], **merged_row)

                        else:
                            order = 'ab'
                            b_coords.pop(0)
                            x = a_coords + b_coords
                            line = LineString(x)
                            merged_row = edge_1
                            merged_row['geometry'] = line
                            merged_row['JNode'] = edge_2['JNode']
                            if G.has_edge(merged_row['INode'], merged_row['JNode']):
                                compare_edge = G.get_edge_data(
                                    merged_row['INode'], merged_row['JNode'])
                                if list(compare_edge['geometry'].coords) == x:
                                    print 'True'
                                    G.remove_edge(edges[0][0], edges[0][1])
                                    G.remove_edge(edges[1][0], edges[1][1])
                            else:
                                G.remove_edge(edges[0][0], edges[0][1])
                                G.remove_edge(edges[1][0], edges[1][1])
                                G.add_edge(merged_row['INode'],
                                       merged_row['JNode'], **merged_row)

                    # Are lines digitized towards each other:
                    elif edge_1['JNode'] == edge_2['JNode']:
                        # Flip the b line
                        b_coords.reverse()
                        # Drop the duplicate coord
                        b_coords.pop(0)
                        x = a_coords + b_coords
                        line = LineString(x)
                        merged_row = edge_1
                        merged_row['geometry'] = line
                        merged_row['INode'] = edge_1['INode']
                        merged_row['JNode'] = edge_2['INode']
                        if G.has_edge(merged_row['INode'], merged_row['JNode']):
                            compare_edge = G.get_edge_data(
                                    merged_row['INode'], merged_row['JNode'])
                            if list(compare_edge['geometry'].coords) == x:
                                print 'True'
                                G.remove_edge(edges[0][0], edges[0][1])
                                G.remove_edge(edges[1][0], edges[1][1])
                        else:
                            G.remove_edge(edges[0][0], edges[0][1])
                            G.remove_edge(edges[1][0], edges[1][1])
                            G.add_edge(merged_row['INode'],
                                   merged_row['JNode'], **merged_row)

                    # Lines must be digitized away from each other:
                    else:
                        # Drop the duplicate coord
                        b_coords.pop(0)
                        # Flip the b line
                        b_coords.reverse()
                        x = b_coords + a_coords
                        line = LineString(x)
                        merged_row = edge_1
                        merged_row['geometry'] = line
                        merged_row['INode'] = edge_2['JNode']
                        merged_row['JNode'] = edge_1['JNode']
                        if G.has_edge(merged_row['INode'], merged_row['JNode']):
                            compare_edge = G.get_edge_data(
                                    merged_row['INode'], merged_row['JNode'])
                            if list(compare_edge['geometry'].coords) == x:
                                G.remove_edge(edges[0][0], edges[0][1])
                                G.remove_edge(edges[1][0], edges[1][1])
                        else:
                            G.remove_edge(edges[0][0], edges[0][1])
                            G.remove_edge(edges[1][0], edges[1][1])
                            G.add_edge(merged_row['INode'],
                                   merged_row['JNode'], **merged_row)

            i = i + 1

        edge_list = []
        for x in G.edges.iteritems():
            edge_list.append(x[1])
        gdf =gpd.GeoDataFrame(edge_list)
        gdf = gdf.append(one_way_keep[cols])

        return(gdf)

    def _thin_junctions(self):
        '''
        Returns junctions/nodes that
        connect thinned edges.
        '''

        keep_nodes = list(set(
            self.thinned_edges_gdf['INode'
                                   ].tolist() + self.thinned_edges_gdf[
                                       'JNode'].tolist()))

        thinned_junctions = self.junctions_gdf[
            self.junctions_gdf['PSRCjunctI'].isin(keep_nodes)]

        thinned_junctions['ScenarioNodeID'] = thinned_junctions[
            'PSRCjunctI'] + self.config['node_offset']

        thinned_junctions['ScenarioNodeID'] = np.where(
            thinned_junctions['EMME2nodeI'
                              ] > 0, thinned_junctions[
                                  'EMME2nodeI'], thinned_junctions[
                                      'ScenarioNodeID'])
        # Now make a map of old to new
        recode_dict = pd.Series(thinned_junctions.ScenarioNodeID.values,
                                thinned_junctions.PSRCjunctI.values).to_dict()
        # Recode_edges
        self.thinned_edges_gdf['NewINode'] = self.thinned_edges_gdf['INode']
        self.thinned_edges_gdf['NewJNode'] = self.thinned_edges_gdf['JNode']
        self.thinned_edges_gdf['NewINode'].replace(recode_dict, inplace=True)
        self.thinned_edges_gdf['NewJNode'].replace(recode_dict, inplace=True)

        return thinned_junctions

    def _check_edge_connection_validity(self, node, edges, network_graph):
        '''
        Checks to make sure edges have a 
        valid connection.
        '''

        if len(edges) != 2:
            self._logger.warning('WARNING: Node ' + str(node) +
                                 ' does not have exactly two edges!')
            return None

        else:
            edge_1 = network_graph.get_edge_data(
                edges[0][0], edges[0][1]).copy()

            edge_2 = network_graph.get_edge_data(
                edges[1][0], edges[1][1]).copy()

            if edge_1['geometry'].type == 'MultiLineString':
                self._logger.warning('WARNING: edge ' +
                                     str(edge_1['PSRCEdgeID']) +
                                     ' is a multipart feature. Please fix.')
                return None

            elif edge_2['geometry'].type == 'MultiLineString':
                self._logger.warning('WARNING: edge ' +
                                     str(edge_2['PSRCEdgeID']) +
                                     ' is a multipart feature. Please fix.')
                return None

            else:
                a_coords = list(edge_1['geometry'].coords)
                b_coords = list(edge_2['geometry'].coords)
                # get the first and last coord for the two edges
                a_test = [a_coords[0], a_coords[-1]]
                b_test = [b_coords[0], b_coords[-1]]
                if len(list(set(a_test).intersection(b_test))) == 0:
                    self._logger.info('WARNING: ' +
                                      str(edge_1['PSRCEdgeID']) + " " +
                                      str(edge_2['PSRCEdgeID']) +
                                      " are not connected!")
                    return None

                else:
                    return (edge_1, edge_2)
