import time
import geopandas as gpd
import pandas as pd
import os
import numpy as np
from shapely.geometry import LineString
from shapely.geometry import Point
#import data_sources
import yaml
from FlagNetworkFromProjects import *
from ThinNetwork import *
from CreateTimeOfDayNetworks import *
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


model_year = config['model_year']

#logger.info(" %s starting updated network from projects", datetime.datetime.today().replace(microsecond=0))
#test = FlagNetworkFromProjects(gdf_TransRefEdges, gdf_ProjectRoutes, gdf_Junctions, config)
#scenario_edges = test.scenario_edges
#logger.info(" %s finished updating network from projects", datetime.datetime.today().replace(microsecond=0))

scenario_edges = gdf_TransRefEdges[((gdf_TransRefEdges.InServiceD <= config['model_year']) 
                                      & (gdf_TransRefEdges.ActiveLink > 0) 
                                      & (gdf_TransRefEdges.ActiveLink <> 999))]
scenario_edges['projRteID'] = 0

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


# Get nodes/edges that cannot be thinned
turn_edge_list = edges_from_turns(gdf_TurnMovements)
no_thin_node_list = nodes_from_edges(turn_edge_list, scenario_edges)
#transit_node_list = nodes_from_transit(gdf_TransitPoints)
#no_thin_node_list = list(set(no_thin_node_list + transit_node_list))

# Get potential nodes to be thinned:
potential_thin_nodes = get_potential_thin_nodes(scenario_edges)
potential_thin_nodes = [x for x in potential_thin_nodes if x not in no_thin_node_list]

logger.info(" %s Potential nodes to thin", len(potential_thin_nodes))

test = ThinNetwork(scenario_edges, gdf_Junctions, potential_thin_nodes, config)
scenario_edges = test.thinned_network_gdf
scenario_junctions = test.thinned_junctions_gdf

test = CreateTimeOfDayNetworks(scenario_edges, scenario_junctions, 'AM', config)

#lenHOV = 0.01 * 5280
#Leng = getWeaveLen(lType)
#        moveX = (Math.Sqrt(Leng)) / 2
#        moveX = Math.Sqrt(moveX)
#        moveY = moveX 'move the same amount as x

#        g_xMove = moveX
#        g_yMove = moveY

 

#def _apply_parallel_offset(geometry):
#    coords = geometry.coords
#    return LineString([(x[0] + 3.63, x[1] + 3.63) for x in coords])
    
#def _get_from_coords(geometry):
#    return geometry.coords[0]
#def _get_to_coords(geometry):
#    return geometry.coords[-1]
#def _shift_junctions(geometry, distance):
#    coord = geometry.coords
#    return Point([(x[0] + distance, x[1] + distance) for x in coord])

#hov_edges = scenario_edges[(scenario_edges.IJLanesHOVAM > 0) | (scenario_edges.JILanesHOVAM > 0)]
#test = hov_edges.geometry.apply(_apply_parallel_offset)
#hov_edges.update(test)
#hov_i = hov_edges.geometry.apply(_get_from_coords)
#hov_j = hov_edges.geometry.apply(_get_to_coords)
##scenario_edges.ix[scenario_edges.index[0]].geometry.parallel_offset(50, 'left')

scenario_edges = pd.concat([scenario_edges, pd.DataFrame(test.hov_weave_edges)])
scenario_edges = pd.concat([scenario_edges, pd.DataFrame(test.hov_edges)])

scenario_junctions =  pd.concat([scenario_junctions, test.hov_junctions])

scenario_junctions.to_file('d:/scenario_junctions1.shp')
scenario_edges.to_file('d:/scenario_edges1.shp')


#scenario_edges.to_file(r'R:\Stefan\GDB_data\ScenarioEdges3.shp')

end_time = datetime.datetime.now()
elapsed_total = end_time - start_time
logger.info('------------------------RUN ENDING_----------------------------------------------')
logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))