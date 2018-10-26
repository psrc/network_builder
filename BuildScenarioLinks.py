import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np
from shapely.geometry import LineString

class BuildScenarioLinks(object):
    def __init__(self, scenario_edges, scenario_junctions, time_period, config, reversible_both_dir = False, reversible_switch_dir = False):
        self.network_gdf = scenario_edges
        self.junctions_gdf = scenario_junctions
        self.time_period = time_period
        self.config = config
        self.reversible_both_dir = reversible_both_dir
        self.reversible_switch_dir = reversible_switch_dir
        self._logger = log_controller.logging.getLogger('main_logger')
        self.full_network = self.create_full_model_network() 
        self.junctions = self._create_junctions()

    def _create_junctions(self):
        junctions = pd.concat([self.junctions_gdf,pd.DataFrame(columns=self.config['emme_node_columns'])])
        junctions['i'] = junctions['ScenarioNodeID']
        junctions['is_zone'] = np.where(junctions['ScenarioNodeID'] <= int(self.config['max_zone_number']), 1,0).astype(int)
        junctions = junctions[self.config['emme_node_columns'] + ['geometry']]
        return junctions

    def create_full_model_network(self):
        # add columns for emme
        network = pd.concat([self.network_gdf,pd.DataFrame(columns=self.config['emme_link_columns'])])
        
        # because a two way GP lane does not always have two way GP
        # JI hov oneway attributes will be configured below
        hov_edges = self._update_hov_oneway(network)
        network.update(hov_edges)
        
        # upadte all attributes, then do special ones later
        network = self._configure_standard_attributes(network, self.config['standard_links'])
        
        # switch one way JI to IJ
        ji_edges = self._switch_oneway_ji(network)
        # configure oneway JI attributes
        ji_edges = self._configure_standard_attributes(ji_edges, self.config['standard_links'])
        network.update(ji_edges)

        # create reverse links for two way streets
        reverse_links = self._create_reverse_links(network)
        reverse_links = self._configure_standard_attributes(reverse_links, self.config['standard_links'])
        network = pd.concat([network, reverse_links])
        network.reset_index(inplace=True)

        # configure HOV attributes
        hov_edges = network[network['FacilityTy'] == 999]
        hov_edges = self._configure_hov_attributes(hov_edges)
        network.update(hov_edges)

        # configure ferry, rail
        network = self._configure_transit_links(network)
        
        # reverse reversibles?
        if self.reversible_switch_dir:
            reversibles = self._reverse_reversibles(network)
            reversibles = self._configure_standard_attributes(reversibles, self.config['standard_links'])
            network.update(reversibles)

        #create reverse walk links on one_way arterials/collectors
        reverse_walk_links = self._create_reverse_walk_links(network)
        reverse_walk_links = self._configure_emme_walk_attributes(reverse_walk_links, self.config['walk_links'])
        network = pd.concat([network, reverse_walk_links])
        network.reset_index(drop = True, inplace=True)
        
        # configure weave link attributes
        weave_links = network[network['FacilityTy']==98]
        weave_links = self._configure_weave_link_attributes(weave_links, self.config['weave_links'])
        network.update(weave_links)

        network = network[self.config['emme_link_columns'] + self.config['additional_keep_columns']]
        network.i = network.i.astype(int)
        network.j = network.j.astype(int)
        network['id'] = network.i.astype(str) + '-' + network.j.astype(str)
        network.set_index(network.id, inplace = True)
       
        network = self._validate_network(network)
        
        return network

    def _update_hov_oneway(self, network):
        # A two way GP lane does not always have two way GP
        hov_edges = network[network['FacilityTy'] == 999]
        # One way IJ HOV:
        hov_edges['Oneway'] = np.where((hov_edges['Oneway']==2) & (hov_edges['IJLanesHOV' + self.time_period] > 0) & (hov_edges['JILanesHOV' + self.time_period] == 0), 0, hov_edges['Oneway'])
        # One way JI HOV
        hov_edges['Oneway'] = np.where((hov_edges['Oneway']==2) & (hov_edges['IJLanesHOV' + self.time_period] == 0) & (hov_edges['JILanesHOV' + self.time_period] > 0), 1, hov_edges['Oneway'])
        # Can have IJ GP and JI hov or vice versa
        hov_edges['Oneway'] = np.where((hov_edges['Oneway']==0) & (hov_edges['IJLanesHOV' + self.time_period] == 0) & (hov_edges['JILanesHOV' + self.time_period] > 0), 1, hov_edges['Oneway'])
        hov_edges['Oneway'] = np.where((hov_edges['Oneway']==1) & (hov_edges['IJLanesHOV' + self.time_period] > 0) & (hov_edges['JILanesHOV' + self.time_period] == 0), 0, hov_edges['Oneway'])
        return hov_edges
    
    def _switch_oneway_ji(self, network):
        ji_edges = network[network['Oneway'] == 1]
        flipped_geom = ji_edges.geometry.apply(self._flip_edges)
        # update with flipped geometry
        ji_edges.geometry.update(flipped_geom)
        cols = self._switch_attributes_dict()
        
        ji_edges = ji_edges.rename(columns = cols)
        ji_edges['Oneway'] = 0
        return ji_edges
        

    def _reverse_reversibles(self, network):
        reversibles = network[network['Oneway'] == 3]
        flipped_geom = reversibles.geometry.apply(self._flip_edges)
        # update reversibles with flipped geometry
        reversibles.geometry.update(flipped_geom)
        # now switch attributes
        #switch_columns = [x[1] + x[0] + x[2:] for x in self.config['dir_columns']]
        #rename_dict = dict(zip(self.config['dir_columns'], switch_columns))
        ## also INode and Jnode
        #rename_dict['NewINode'] = 'NewJNode'
        #rename_dict['NewJNode'] = 'NewINode'
        reversibles = reversibles.rename(columns = self._switch_attributes_dict())
        return reversibles
        


    def _create_reverse_links(self, network):
        if self.reversible_both_dir:
            two_way_edges = network[(network['Oneway'] == 2) | (network['Oneway'] == 3)]
        else:
            two_way_edges = network[network['Oneway'] == 2]
        #flip geometry
        flipped_geom = two_way_edges.geometry.apply(self._flip_edges)
        two_way_edges.geometry.update(flipped_geom)
        # flip attributes:
        #switch_columns = [x[1] + x[0] + x[2:] for x in self.config['dir_columns']]
        #rename_dict = dict(zip(self.config['dir_columns'], switch_columns))
        ## also INode and Jnode
        #rename_dict['NewINode'] = 'NewJNode'
        #rename_dict['NewJNode'] = 'NewINode'
        # switch IJ & JI attributes for edges that are digitized in the opposite direction of the project
        cols = self._switch_attributes_dict()
        two_way_edges = two_way_edges.rename(columns = cols)
        return two_way_edges


    def _create_reverse_walk_links(self, network):
        reverse_walk_links = network[network.NewFacilit.isin(self.config['reverse_walk_link_facility_types'])]
        reverse_walk_links = reverse_walk_links[(reverse_walk_links['Oneway'] == 0) | (reverse_walk_links['Oneway'] == 1)]
        flipped_geom = reverse_walk_links.geometry.apply(self._flip_edges)
        reverse_walk_links.geometry.update(flipped_geom)
        cols = self._switch_attributes_dict()
        reverse_walk_links = reverse_walk_links.rename(columns = cols)
        # set these these all to oneway IJ-dont want them to get picked up by a JI query later on 
        reverse_walk_links['Oneway'] = 0
        return reverse_walk_links
        
    def _flip_edges(self, geometry):
        line = list(geometry.coords)
        line.reverse()
        return LineString(line)

    def _switch_attributes_dict(self):
        switch_columns = [x[1] + x[0] + x[2:] for x in self.config['dir_columns']]
        rename_dict = dict(zip(self.config['dir_columns'], switch_columns))
        # also INode and Jnode
        rename_dict['NewINode'] = 'NewJNode'
        rename_dict['NewJNode'] = 'NewINode'
        return rename_dict

    def _configure_emme_walk_attributes(self, edges, look_up_dict):
        edges.direction = int(look_up_dict['direction'])
        edges.i = edges[look_up_dict['i']]
        edges.j = edges[look_up_dict['j']]
        edges.length = edges.geometry.length/5820.0
        edges.modes = look_up_dict['modes']
        edges.type = look_up_dict['type']
        edges.lanes = look_up_dict['lanes']
        edges.vdf = look_up_dict['vdf']
        edges.ul1 = look_up_dict['ul1']
        edges.ul2 = look_up_dict['ul2']
        edges.ul3 = look_up_dict['ul3']
        return edges

    def _configure_weave_link_attributes(self, edges, look_up_dict):
        edges.direction = int(look_up_dict['direction'])
        edges.i = edges[look_up_dict['i']]
        edges.j = edges[look_up_dict['j']]
        edges.length = look_up_dict['length']/5820.0
        edges.modes = look_up_dict['modes']
        edges.type = look_up_dict['type']
        edges.lanes = look_up_dict['lanes']
        edges.vdf = look_up_dict['vdf']
        edges.ul1 = look_up_dict['ul1']
        edges.ul2 = look_up_dict['ul2']
        edges.ul3 = look_up_dict['ul3']
        return edges

    def _configure_standard_attributes(self, edges, look_up_dict):
        edges.direction = int(look_up_dict['direction'])
        edges.i = edges[look_up_dict['i']]
        edges.j = edges[look_up_dict['j']]
        edges.length = edges.geometry.length/5820.0
        edges.modes = edges[look_up_dict['modes']]
        edges.type = edges[look_up_dict['type']]
        edges.lanes = edges[look_up_dict['lanes'] + self.time_period]
        edges.vdf = edges[look_up_dict['vdf']]
        edges.ul1 = edges[look_up_dict['ul1']]
        edges.ul2 = edges[look_up_dict['ul2']]
        edges.ul3 = edges[look_up_dict['ul3']]
        return edges

    def _configure_hov_attributes(self, edges):
        
        # first recode modes to the HOV lanes field, which is a lookup for type of HOV and number of lanes
        edges.modes = edges['IJLanesHOV' + self.time_period]
        # Now use dict to recode
        edges.modes.replace(self.config['hov_modes'], inplace=True)
        edges.lanes = edges['IJLanesHOV' + self.time_period]
        edges.lanes.replace(self.config['hov_lanes'], inplace=True)
        edges.ul1 = edges[self.config['hov_capacity']]
        edges.i = edges.NewINode
        edges.j = edges.NewJNode
        return edges

    def _configure_transit_links(self, edges):
        edges.ul2 = edges.ul2.astype(float)
        edges.ul2 = np.where(edges['Modes'].isin(self.config['link_time_modes']), edges.Processing_x/1000.0, edges.ul2)
        return edges

    def _validate_network(self, edges):
        if len(edges[edges['type'] == 0]) > 0:
            self._logger.warning('Warning: Field LinkType in TransRefEdges containes 0s. Recoding to 90.')
            edges['type'] = np.where(edges['type'] == 0, 90, edges['type'])
        # fix this
        edges['type'] = np.where(edges['type'] > 90, 90, edges['type'])

        loops = edges[edges['i'] == edges['j']].PSRCEdgeID.tolist()
        if loops:
            for edge_id in loops:
                self._logger.warning('Warning: Edge %s has the same i and j node.' % (edge_id))

        empty_mode = edges[edges['modes'] == ' '].PSRCEdgeID.tolist()
        if empty_mode:
            for edge_id in empty_mode:
                self._logger.warning('Warning: Edge %s mode field is blank. Please fix!' % (edge_id))
        return edges
        

