import time
import geopandas as gpd
import pandas as pd
import os
import numpy as np
from shapely.geometry import LineString
#import data_sources
import yaml
from FlagNetworkFromProjects import *
from ThinNetwork import *
import logging
import log_controller
import datetime
import networkx as nx
from networkx.algorithms.components import *
import collections

config = yaml.safe_load(open("config.yaml"))
#start = time.time()

logger = log_controller.setup_custom_logger('main_logger')
logger.info('------------------------Network Builder Started----------------------------------------------')
start_time = datetime.datetime.now()

logger.info(" %s starting data import", datetime.datetime.today().replace(microsecond=0))
from data_sources import *
logger.info(" %s finished data import", datetime.datetime.today().replace(microsecond=0))


config = yaml.safe_load(open("R:/Stefan/GDB_data/code/config.yaml"))

model_year = config['model_year']

test = FlagNetworkFromProjects(gdf_TransRefEdges, gdf_ProjectRoutes, gdf_Junctions, config)
scenario_edges = test.scenario_edges


def edges_from_turns(turns):
    edge_list = turns.FrEdgeID.tolist()
    edge_list =  edge_list + gdf_TurnMovements.ToEdgeID.tolist()
    return edge_list

def nodes_from_transit(transit_points):
    return set(transit_points.PSRCJunctI.tolist)

def nodes_from_edges(list_of_edges, edges):
    edges = edges[edges.PSRCEdgeID.isin(list_of_edges)]
    node_list = edges.INode.tolist()
    print len(node_list)
    node_list = node_list + edges.JNode.tolist()
    return list(set(node_list))

def get_potential_thin_nodes(edges):
    node_list = edges.INode.tolist() + edges.JNode.tolist()
    df = pd.DataFrame(pd.Series(node_list).value_counts(), columns=['node_count'])
    df = df[df.node_count == 2]
    return df.index.tolist()

def merge_edges(geom1, geom2, path_dir, edge1_dir, edge2_dir):
    if geom1.type =='MultiLineString':
        print 'edge ' + str(int(a_row.PSRCEdgeID)) + ' is a multipart feature. Please fix.'  
    elif geom2.type =='MultiLineString':
        print 'edge ' + str(int(b_row.PSRCEdgeID)) + ' is a multipart feature. Please fix.'  
    else:
        a_coords = list(geom1.coords)
        b_coords = list(geom2.coords)
    #if edge1 is in the opposite dir, flip it:
    if edge1_dir <> path_dir:
        a_coords.reverse()
    if path_dir == 'IJ':
        if  edge2_dir == 'IJ':
            b_coords.pop(0)
            x = a_coords + b_coords 
            line = LineString(x)
        else:
            #Flip the b line
             b_coords.reverse()
             # drop the duplicate coord
             b_coords.pop(0)
             x = a_coords + b_coords
             line = LineString(x)
    else:
        if edge2_dir == 'JI':
            a_coords.pop(0)
            x = b_coords + a_coords 
            line = LineString(x)
        else:
            a_coords.pop(0)
            #Flip the b line
            b_coords.reverse()
            x = b_coords + a_coords 
            line = LineString(x)
    return line 


# Get nodes/edges that cannot be thinned
turn_edge_list = edges_from_turns(gdf_TurnMovements)
no_thin_node_list = nodes_from_edges(turn_edge_list, scenario_edges)
#transit_node_list = nodes_from_transit(gdf_TransitPoints)
#no_thin_node_list = list(set(no_thin_node_list + transit_node_list))

# Get potential nodes to be thinned:
potential_thin_nodes = get_potential_thin_nodes(scenario_edges)
potential_thin_nodes = [x for x in potential_thin_nodes if x not in no_thin_node_list]

logger.info(" %s Potential nodes to thin", len(potential_thin_nodes))

test = ThinNetwork(scenario_edges, potential_thin_nodes)
#scenario_edges = test.thinned_network_gdf

scenario_edges.to_file(r'R:\Stefan\GDB_data\ScenarioEdges3.shp')

end_time = datetime.datetime.now()
elapsed_total = end_time - start_time
logger.info('------------------------RUN ENDING_----------------------------------------------')
logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))