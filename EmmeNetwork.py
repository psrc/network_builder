import numpy as np
import pandas as pd
import inro.emme.desktop.app as app
import inro.modeller as _m
import inro.emme.matrix as ematrix
import inro.emme.database.matrix
import inro.emme.database.emmebank as _eb
import geopandas as gpd
import os
from EmmeProject import *
import json
import log_controller

class EmmeNetwork(object):
    def __init__(self, emme_project, time_period, transit_lines, model_links, model_nodes, turns,  config, transit_segments = None):
        self.emme_project = emme_project
        self.time_period = time_period
        self.links = model_links
        self.nodes = model_nodes
        self.turns = turns
        self.transit_segments = transit_segments
        #transit_lines = transit_lines[transit_lines.InServiceD==2014]
        transit_lines = transit_lines.loc[transit_lines['Headway_' + self.time_period] > 0]
        self.transit_lines = transit_lines
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
    
    def _create_scenario(self, scenario_name, scenario_id):
        scenario = self.emme_project.bank.create_scenario(scenario_id)
        scenario.title = scenario_name
        return scenario
        #network = scenario.get_network()
        #need to have at least one mode defined in scenario. Real modes are imported in network_importer.py
        #network.create_mode('AUTO', 'a')
        #scenario.publish_network(network)
        #self.emme_project.change_scenario(scenario_id)


    def load_network(self):
        scenario_id = self.config['time_periods'].index(self.time_period) + 1
        scenario = self._create_scenario(self.time_period, scenario_id)
        self.emme_project.process_modes(self.config['data_path'] + '\\' +  self.config['modes_file'], scenario)
        self.emme_project.process_vehicles(self.config['data_path'] + '\\' +  self.config['transit_vehicle_file'], scenario)
        #('inputs/scenario/networks/' + self.config['transit_vehicle_file'] , self.emme_project.bank.scenario(scenario_id))
        self._load_network_elements(scenario)

    def _load_network_elements(self, scenario):
        network =  scenario.get_network()
        for node in self.nodes.iterrows():
            node = node[1]
            if node.i <= 4000:
                emme_node = network.create_centroid(node.i)
            else:
                emme_node = network.create_regular_node(node.i)
            emme_node.x = node.geometry.x
            emme_node.y = node.geometry.y

        for link in self.links.iterrows():
            link = link[1]
            emme_link = network.create_link(link.i, link.j, link.modes)
            emme_link.type = int(link.type)
            emme_link.num_lanes = int(link.lanes)
            emme_link.volume_delay_func = int(link.vdf)
            emme_link.data1 = int(link.ul1)
            emme_link.data2 = int(link.ul2)
            emme_link.data3 = int(link.ul3)
            emme_link.vertices = vertices = list(link.geometry.coords)[1:-1]

        for i in self.turns.j_node.unique():
            if network.node(i):
                test =  network.create_intersection(i)
            else:
                self._logger.warning('Could not find find node %s in %s network!' % (i, self.time_period))
        for turn in network.turns():
            print turn.id
        for turn in self.turns.iterrows():
            turn = turn[1]
            #test = network.create_intersection(turn.i_node)
            emme_turn = network.turn(turn.i_node, turn.j_node, turn.k_node)
            if emme_turn:
                emme_turn.penalty_func = 0
            else :
                self._logger.warning('Could not find find turn %s, %s, %s in %s network!' % (turn.i_node, turn.j_node, turn.k_node, self.time_period))

        if self.transit_segments is not None:
            self.transit_segments.set_index('seg_id', inplace = True)
            for line in self.transit_lines.iterrows():
                line = line[1]
                segs = self.transit_segments.loc[self.transit_segments.route_id == line.LineID]
                if segs.empty:
                    self._logger.warning('No transit segments for line %s in %s segment table!' % (line.LineID, self.time_period))
                    continue
                elif len(segs) == 1:
                    emme_line = network.create_transit_line(line.LineID, line.VehicleTyp , [segs.INode, segs.JNode])
                else:
                    nodes = segs.INode.tolist() + [segs.JNode.tolist()[-1]]
                    emme_line = network.create_transit_line(line.LineID, line.VehicleTyp , nodes)
                emme_line.description = line.Descriptio
                emme_line.speed = line.Speed
                emme_line.headway = line['Headway_' + self.time_period]
                emme_line.data1 = line.Processing
                emme_line.data3 = line.Operator 

            x = 0
            for line in network.transit_lines():
                x = x + 1
                for seg in line.segments():
                    row = self.transit_segments.ix[seg.id]
                    seg.transit_time_func = row.ttf
                    if line.mode == 'f' or line.mode == 'r' or line.mode == 'c':
                        seg.allow_alightings = True
                        seg.allow_boardings = True
                        seg.dwell_time = 0
                    if row.is_stop:
                        seg.allow_alightings = True
                        seg.allow_boardings = True
                        seg.dwell_time = .25
                    else:
                        seg.allow_alightings = False
                        seg.allow_boardings = False
                        seg.dwell_time = False

        scenario.publish_network(network)


