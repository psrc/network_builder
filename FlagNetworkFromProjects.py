import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np


class FlagNetworkFromProjects(object):

    def __init__(self, network_gdf, projects_gdf, junctions_gdf, config):
        self.network_gdf = network_gdf
        self.project_gdf = projects_gdf
        self.junctions_gdf = junctions_gdf
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
        self._logger.info('logger started!')

        self.route_edges = self._get_route_edges()
        self.route_junctions = self._get_route_junctions()
        self.route_junctions_coords = self._junctions_to_coords()
        self.route_edges_dict = self._edges_to_dict()
        self.flagged_edges = self._flag_edges()
        self.scenario_edges = self._update_edge_attributes()

    def _get_route_edges(self):
        # Get all edges associted with routes (project or transit)
        buff_projects = self.project_gdf.copy()
        buff_projects['geometry'] = self.project_gdf.geometry.buffer(self.config['project_buffer_dist'])
        edges = gpd.sjoin(self.network_gdf, buff_projects, how='inner',
                          op='within')
        # Overlapping projects will result in more than one edge of
        # the same id.Need to figure out how to handle this. Get first
        # for now.
        edges = edges.sort_values(by=['InServiceDate'], ascending=False)
        edges = gpd.GeoDataFrame(edges.groupby('PSRCEdgeID').first())
        edges.reset_index(inplace=True)
        return edges

    def _get_route_junctions(self):
        # Get a list of all Junctions associated with route
        # edtes(project or transit)
        node_list = self.route_edges.INode.tolist()
        node_list = list(set(node_list +
                             self.route_edges.JNode.tolist()))
        return self.junctions_gdf[self.junctions_gdf.PSRCjunctI.isin
                                  (node_list)]

    def _junctions_to_coords(self):
        # Returns a dictionary where the key is PSRCJunctID and
        # the values are its coords in tuple.
        self.route_junctions
        return {x.geometry.coords[0]:
                x.PSRCjunctI for x in self.route_junctions.itertuples()}

    def _edges_to_dict(self):
        # Returns a dictionary where the key is the edge IJ pair and the
        # values are its ID and direction.
        edge_dict = {}
        for edge in self.route_edges.itertuples():
            edge_coords = [(x[0], x[1]) for x in edge.geometry.coords]

            # Oneway IJ or reversible
            if edge.Oneway == 0 or edge.Oneway == 1:
                edge_dict[(edge.INode, edge.JNode)] = {'PSRCEdgeID':
                                                       edge.PSRCEdgeID,
                                                       'dir': 'IJ'}
            # Two way
            elif edge.Oneway == 2:
                edge_dict[(edge.INode, edge.JNode)] = {'PSRCEdgeID':
                                                       edge.PSRCEdgeID,
                                                       'dir': 'IJ'}
                edge_dict[(edge.JNode, edge.INode)] = {'PSRCEdgeID':
                                                       edge.PSRCEdgeID,
                                                       'dir': 'JI'}
            # Oneway JI
            elif edge.Oneway == 3:
                edge_dict[(edge.JNode, edge.INode)] = {'PSRCEdgeID':
                                                       edge.PSRCEdgeID,
                                                       'dir': 'JI'}

        return edge_dict

    def _flag_edges(self):
        # get unique projects in a list
        route_id_list = list(set(self.route_edges.projRteID.tolist()))
        proj_edge_list = []
        for route_id in route_id_list:
            # Only certain edges associated with this project should
            # be updated because of potential overlapping projects
            update_edges = self.route_edges[self.route_edges.projRteID ==
                                            route_id].PSRCEdgeID.tolist()
            # get the project line work
            project = self.project_gdf[self.project_gdf.projRteID == route_id]
            if project.ix[project.index[0]].geometry.type == 'MultiLineString':
                print 'Project ' + str(int(project.projRteID)) + ' MultiLineString'
            else:
                # tuple containing sequence of project coords
                project_coords = [(x[0], x[1]) for x in project.ix
                              [project.index[0]].geometry.coords]
                node_list = []
                for coord in project_coords:
                    # add only coords that are spatialy coincident with junctions
                    if coord in self.route_junctions_coords.keys():
                        node_list.append(self.route_junctions_coords[coord])
                # get edges from node sequence:
                while len(node_list) > 1:
                    # Check that the edge has been selected in spatial join op
                    if (node_list[0], node_list[1]) in self.route_edges_dict.keys():
                        # get the edge info, stored in a dictionary
                        edge_dict = self.route_edges_dict[node_list[0], node_list[1]]
                        # has the edge dict already been added?
                        if edge_dict in proj_edge_list:
                            self._logger.info( 'Edge ' + str(node_list[0]) + '-' + str(node_list[1])  + ' already tagged, check ' + str(int(project.projRteID)) + ' to see if digizited correctly.')
                        # Check to see if this edge should be updated with this project
                        elif edge_dict['PSRCEdgeID'] in update_edges:
                            # Sometimes a project that goes back in on itself will select the same edge twice. Prevent this by removing the edge from update_edges. 
                            update_edges.remove(edge_dict['PSRCEdgeID'])
                            # add the projRteID so we know which project updates this edge
                            edge_dict['projRteID'] = route_id
                            proj_edge_list.append(edge_dict)
                        node_list.pop(0)
                    else:
                        # Think about adding these as they are most likely valid edges that did not get selected because of bad Project line work
                        self._logger.info('Edge ' + str(node_list[0]) + '-' + str(node_list[1])  + ' was not selected as part of project ' + str(int(project.projRteID)) + '. Check to make sure project route is covering edges.')
                        node_list.pop(0)

        return pd.DataFrame(proj_edge_list)

    def _update_edge_attributes(self):
        # Get the edges used in the model
        project_edges =  self.flagged_edges.merge(self.project_gdf, on = 'projRteID', how = 'left')

        scenario_edges = self.network_gdf[((self.network_gdf.InServiceD <= self.config['model_year']) 
                                      & (self.network_gdf.ActiveLink > 0) 
                                      & (self.network_gdf.ActiveLink <> 999)) | self.network_gdf.PSRCEdgeID.isin(project_edges.PSRCEdgeID.tolist())]

        # break ij and ji projects into two DFs
        ij_proj_edges = project_edges[project_edges.dir == "IJ"].copy()
        ji_proj_edges =  project_edges[project_edges.dir == "JI"].copy()
        # get all the directional columns
        dir_columns = [x for x in ij_proj_edges if x[0:2] == "IJ" or x[0:2] == "JI" ]
        # switch IJ and JI columns
        switch_columns = [x[1] + x[0] + x[2:] for x in dir_columns]
        rename_dict = dict(zip(dir_columns, switch_columns))
        # switch IJ & JI attributes for edges that are digitized in the opposite direction of the project
        ji_proj_edges = ji_proj_edges.rename(columns = rename_dict)
        # add a column to hold the project route id:
        scenario_edges['projRteID'] = 0
        # merge the edge DFs back together
        merged_projects = pd.concat([ji_proj_edges, ij_proj_edges])
        merged_projects.set_index('PSRCEdgeID', inplace = True)

        merged_projects = merged_projects[self.config['project_update_attributes'] + dir_columns]
        scenario_edges.set_index('PSRCEdgeID', inplace = True)
        merged_projects.replace(-1, np.NaN, inplace = True)
        scenario_edges.update(merged_projects)
        scenario_edges.reset_index(inplace = True)
        return scenario_edges

