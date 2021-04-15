import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np
import sys


class FlagNetworkFromProjects(object):

    def __init__(self, network_gdf, projects_gdf, junctions_gdf, config):
        self.network_gdf = network_gdf
        self.project_gdf = projects_gdf
        self.junctions_gdf = junctions_gdf
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
        self.route_edges = self._get_route_edges()
        self.edge_proj_dict = self._edge_project_map()
        self.route_junctions = self._get_route_junctions()
        self.route_junctions_coords = self._junctions_to_coords()
        self.route_edges_dict = self._edges_to_dict()
        self.valid_edges = self._valid_edges()
        self.flagged_edges = self._flag_edges()
        self.scenario_edges = self._update_edge_attributes()

    def _get_route_edges(self):
        '''
        Returns a GeoDataFrame of edges that are covered by projects.
        The project with the farthest horizion date is used in cases
        where an edge is updated by more than one project.
       '''

        buff_projects = self.project_gdf.copy()
        buff_projects['geometry'] = self.project_gdf.geometry.buffer(
            self.config['project_buffer_dist'])

        edges = gpd.sjoin(self.network_gdf, buff_projects, how='inner',
                          op='within')

        edges = edges.sort_values(by=['InServiceDate_right'], ascending=False)

        edges['freq'] = edges.groupby(
            'PSRCEdgeID')['PSRCEdgeID'].transform('count')

        # Warn users
        dup_edges = edges.loc[edges.freq > 1]
        dup_edges_dict = dup_edges.groupby(['PSRCEdgeID']).apply(
            lambda x: list(x.projRteID)).to_dict()

        for edge_id, proj_ids in dup_edges_dict.items():
            self._logger.info(
                'Warning! Edge %s is covered by more than one project: %s.'
                % (edge_id, proj_ids))

        edges = gpd.GeoDataFrame(edges.groupby('PSRCEdgeID').first())
        edges.reset_index(inplace=True)
        return edges

    def _edge_project_map(self):
        '''
        Returns a dictionary containing the map between edge and project.
        The key is edge id and the value is project id.
       '''
        return self.route_edges.groupby(['PSRCEdgeID']).apply(
            lambda x: list(x.projRteID)).to_dict()

    def _get_route_junctions(self):
        '''
        Returns a lits of all junction that are associated with
        project route edges (edges that are covered by projects).
        '''

        node_list = self.route_edges.INode.tolist()
        node_list = list(set(node_list +
                             self.route_edges.JNode.tolist()))
        return self.junctions_gdf[self.junctions_gdf.PSRCjunctID.isin
                                  (node_list)]

    def _junctions_to_coords(self):
        '''
        Returns a dictionary where the key is junction id and
        the value is a tuple of the junction's coordinates.
        '''

        self.route_junctions
        return {x.geometry.coords[0]:
                x.PSRCjunctID for x in self.route_junctions.itertuples()}

    def _edges_to_dict(self):
        '''
        Returns a nested dictionary where the key is a tuple
        containing the from and to node ID's and the value is a
        dictionary, which contains the edge id it's directionality.
        '''

        edge_dict = {}
        for edge in self.route_edges.itertuples():

            # Oneway IJ or reversible
            if edge.Oneway_left == 0 or edge.Oneway_left == 1:
                edge_dict[(edge.INode, edge.JNode)] = {'PSRCEdgeID':
                                                       edge.PSRCEdgeID,
                                                       'dir': 'IJ'}
            # Two way
            elif edge.Oneway_left == 2:
                edge_dict[(edge.INode, edge.JNode)] = {'PSRCEdgeID':
                                                       edge.PSRCEdgeID,
                                                       'dir': 'IJ'}
                edge_dict[(edge.JNode, edge.INode)] = {'PSRCEdgeID':
                                                       edge.PSRCEdgeID,
                                                       'dir': 'JI'}
            # Oneway JI
            elif edge.Oneway_left == 3:
                edge_dict[(edge.JNode, edge.INode)] = {'PSRCEdgeID':
                                                       edge.PSRCEdgeID,
                                                       'dir': 'JI'}

        return edge_dict

    def _valid_edges(self):
        '''
        Returns a list of tuples containing all valid IJ pairs in the network.
        '''

        edge_list = []
        for edge in self.network_gdf.itertuples():
            if edge.geometry.type == 'MultiLineString':
                self._logger.warning('WARNING: edge ' +
                                     str(edge.PSRCEdgeID) +
                                     ' is a multipart feature. Please fix.')
                sys.exit(1)


            edge_coords = [(x[0], x[1]) for x in edge.geometry.coords]

                # Oneway IJ or reversible
            if edge.Oneway == 0 or edge.Oneway == 1:
                edge_list.append((edge.INode, edge.JNode))

            # Two way
            elif edge.Oneway == 2:
                edge_list.append((edge.INode, edge.JNode))
                edge_list.append((edge.JNode, edge.INode))

            # Oneway JI
            elif edge.Oneway == 3:
                edge_list.append((edge.JNode, edge.INode))

        return edge_list

    def _flag_edges(self):
        '''
        Returns a GeoDataFrame of edges with the project id that updates
        them and the directionality of the edge with repsect to the
        digitized direction of the project. This is required so that
        attributes are applied correctly to the edges beneath them. The
        project with the farthest horizion date is used in cases where
        an edge is updated  by more than one project.
       '''
        route_id_list = list(set(self.route_edges.projRteID.tolist()))
        proj_edge_list = []

        for route_id in route_id_list:
            update_edges = self.route_edges[self.route_edges.projRteID ==
                                            route_id].PSRCEdgeID.tolist()
            project = self.project_gdf[self.project_gdf.projRteID == route_id]

            if project.loc[project.index[0]].geometry.type == 'MultiLineString':
                self._logger.info(
                    'Warning! Project %s is MultiLineString. Cannot update'
                    ' network with this project!' % (
                        route_id))
            else:
                # Tuple containing sequence of project coords
                project_coords = [(x[0], x[1]) for x in project.loc
                                  [project.index[0]].geometry.coords]

                # Add only coords that are spatialy coincident with junctions
                node_list = [self.route_junctions_coords[x] for x in
                             project_coords if x in
                             self.route_junctions_coords.keys()]

                edge_count = 0
                while len(node_list) > 1:
                    # Check to see if node pair make a valid edge
                    if (node_list[0], node_list[1]) in self.valid_edges or (
                            node_list[1], node_list[0]) in self.valid_edges:

                        edge_count = edge_count + 1

                        # Check that the edge has been flagged by proj
                        if (node_list[0], node_list[1]) in \
                                self.route_edges_dict.keys():

                            # Get the edge info, stored in a dictionary
                            edge_dict = self.route_edges_dict[
                                node_list[0], node_list[1]]

                            if edge_dict in proj_edge_list:

                                if edge_dict['projRteID'] == route_id:
                                    self._logger.info(
                                    'Edge %s already tagged by this project'
                                    ' %s check to see if digizited correctly.'
                                    % (edge_dict['PSRCEdgeID'], route_id))

                            elif edge_dict['PSRCEdgeID'] in update_edges:
                                edge_dict['projRteID'] = route_id
                                proj_edge_list.append(edge_dict)
                                update_edges.remove(edge_dict['PSRCEdgeID'])
                            node_list.pop(0)

                        # This should only be the case if the project
                        # is digitized in the wrong direction for a
                        # one-way street/highway.
                        elif (node_list[1], node_list[0]) in \
                                self.route_edges_dict.keys():

                            self._logger.info(
                                    'Project %s seems to be digitized in the'
                                    ' wrong direction. Updating, but should'
                                    ' be fixed'
                                    % (route_id))

                            # Get the edge info, stored in a dictionary
                            edge_dict = self.route_edges_dict[
                                node_list[1], node_list[0]]

                            if edge_dict in proj_edge_list:

                                if edge_dict['projRteID'] == route_id:
                                    self._logger.info(
                                    'Edge %s already tagged by this project'
                                    ' %s check to see if digizited correctly.'
                                    % (edge_dict['PSRCEdgeID'], route_id))

                            elif edge_dict['PSRCEdgeID'] in update_edges:
                                edge_dict['projRteID'] = route_id
                                proj_edge_list.append(edge_dict)
                                update_edges.remove(edge_dict['PSRCEdgeID'])
                            node_list.pop(0)

                        else:
                            # Edge was not selected by project
                            if (node_list[0], node_list[1]) in \
                                    self.route_edges_dict.keys():

                                edge_id = self.route_edges_dict[
                                    node_list[0], node_list[1]]['PSRCEdgeID']

                                if edge_id in self.edge_proj_dict.keys():
                                    self._logger.info(
                                        'Warning! Edge %s already'
                                        ' selected by projRteID %s.'
                                        % (edge_id, self.edge_proj_dict[
                                            edge_id]))
                                else:
                                    self._logger.info(
                                        'Warning! Edge %s was not selected as'
                                        ' part of project %s, but probably'
                                        ' should have. Check to make sure'
                                        ' project covers edges.'
                                        % (edge_id,
                                           self.edge_proj_dict[edge_id]))
                            else:
                                self._logger.info(
                                    'Edge %s-%s was not selected as part of'
                                    ' project. Check to make sure it is'
                                    ' covered by project %s' % (
                                        node_list[0], node_list[1], route_id))
                            node_list.pop(0)
                    else:
                        # Not a valid edge. Pop the 2nd node and try again.
                        node_list.pop(1)

                if edge_count == 0:
                    self._logger.info(
                        'Warning! No edges found as part of project %s.'
                        % (route_id))

        return pd.DataFrame(proj_edge_list)

    def _update_edge_attributes(self):
        '''
        Returns a GeoDataFrame of edges that will be used in the model
        scenario. Edges that have been flagged by projects are updated
        by the projects attributes, where applicable. If an edge is
        digitized in the opposite direction of the project, then it's
        directional attrbutes are switched before updating. So all IJ
        attributes become JI and vise versa.
       '''

        project_edges = self.flagged_edges.merge(
            self.project_gdf, on='projRteID', how='left')

        scenario_edges = self.network_gdf[((
            self.network_gdf.InServiceDate <=
            self.config['model_year']) &
            (self.network_gdf.ActiveLink > 0) &
            (self.network_gdf.ActiveLink != 999)) |
            self.network_gdf.PSRCEdgeID.isin(
            project_edges.PSRCEdgeID.tolist())]

        # Break ij and ji project edges into two DFs
        ij_proj_edges = project_edges[
            project_edges.dir == "IJ"].copy()
        ji_proj_edges = project_edges[
            project_edges.dir == "JI"].copy()

        switch_columns = [x[1] + x[0] + x[2:] for x in self.config[
            'dir_columns']]
        rename_dict = dict(zip(self.config['dir_columns'],
                               switch_columns))

        # Switch IJ & JI attributes for edges that are
        # digitized in the opposite direction of the project
        ji_proj_edges = ji_proj_edges.rename(columns=rename_dict)

        # Add a column to hold the project route id:
        scenario_edges['projRteID'] = 0

        # Merge the edge DFs back together
        merged_projects = pd.concat([ji_proj_edges, ij_proj_edges])
        merged_projects.set_index('PSRCEdgeID', inplace=True)

        merged_projects = merged_projects[
            self.config['project_update_columns'] + self.config['dir_columns']]
        scenario_edges.set_index('PSRCEdgeID', inplace=True)

        # Recode -1s to Nan so they do not update scenario edges
        merged_projects.replace(-1, np.NaN, inplace=True)
        merged_projects.replace('-1', np.NaN, inplace=True)

        # Update scenario_edges with project attribues
        scenario_edges.update(merged_projects)
        scenario_edges.reset_index(inplace=True)
        
        # Delete edges flagged for removal
        self._logger.info('Removing %s edges deleted by projects' % (len(scenario_edges[scenario_edges['FacilityType'] == 99])))
        scenario_edges = scenario_edges.loc[scenario_edges['FacilityType'] != 99].copy()

        return scenario_edges
