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
from BuildHOVSystem import *
from BuildScenarioLinks import *
from CreateTransitSegmentTable import *
import logging
import log_controller
import datetime
import networkx as nx
from networkx.algorithms.components import *
import collections
import multiprocessing as mp 
import build_transit_segments_parallel
import collections
#from multiprocessing import Pool 


def edges_from_turns(turns):
    edge_list = turns.FrEdgeID.tolist()
    edge_list =  edge_list + gdf_TurnMovements.ToEdgeID.tolist()
    return edge_list

def nodes_from_transit(transit_points):
    return list(set(transit_points.PSRCJunctI.tolist()))

def nodes_from_centroids(junctions):
    centroid_junctions = junctions[junctions.EMME2nodeI > 0]
    return centroid_junctions.PSRCjunctI.tolist()

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





if __name__ == '__main__':
    config = yaml.safe_load(open("config.yaml"))
    #start = time.time()

    logger = log_controller.setup_custom_logger('main_logger')
    logger.info('------------------------Network Builder Started----------------------------------------------')
    start_time = datetime.datetime.now()

    logger.info(" %s starting data import", datetime.datetime.today().replace(microsecond=0))
    from data_sources import *
    logger.info(" %s finished data import", datetime.datetime.today().replace(microsecond=0))


    model_year = config['model_year']

    logger.info(" %s starting updated network from projects", datetime.datetime.today().replace(microsecond=0))
    #test = FlagNetworkFromProjects(gdf_TransRefEdges, gdf_ProjectRoutes, gdf_Junctions, config)
    #scenario_edges = test.scenario_edges
    logger.info(" %s finished updating network from projects", datetime.datetime.today().replace(microsecond=0))

    scenario_edges = gdf_TransRefEdges.loc[((gdf_TransRefEdges.InServiceD <= config['model_year']) 
                                      & (gdf_TransRefEdges.ActiveLink > 0) 
                                      & (gdf_TransRefEdges.ActiveLink <> 999))].copy()
    scenario_edges['projRteID'] = 0

    # Get nodes/edges that cannot be thinned
    turn_edge_list = edges_from_turns(gdf_TurnMovements)
    no_thin_node_list = nodes_from_edges(turn_edge_list, scenario_edges)

    centroids = nodes_from_centroids(gdf_Junctions)
    no_thin_node_list = no_thin_node_list + centroids


    transit_node_list = nodes_from_transit(gdf_TransitPoints)
    no_thin_node_list = list(set(no_thin_node_list + transit_node_list))

    # Get potential nodes to be thinned:
    potential_thin_nodes = get_potential_thin_nodes(scenario_edges)
    potential_thin_nodes = [x for x in potential_thin_nodes if x not in no_thin_node_list]

    logger.info(" %s Potential nodes to thin", len(potential_thin_nodes))

    test = ThinNetwork(scenario_edges, gdf_Junctions, potential_thin_nodes, config)
    scenario_edges = test.thinned_network_gdf
    scenario_junctions = test.thinned_junctions_gdf

    test = BuildHOVSystem(scenario_edges, scenario_junctions, 'AM', config)

    scenario_edges = pd.concat([scenario_edges, pd.DataFrame(test.hov_weave_edges)])
    scenario_edges = pd.concat([scenario_edges, pd.DataFrame(test.hov_edges)],)
    # need to reset so we dont have duplicate index values
    scenario_edges.reset_index(inplace = True)

    scenario_junctions =  pd.concat([scenario_junctions, test.hov_junctions])
    scenario_junctions.reset_index(inplace = True)

    test = BuildScenarioLinks(scenario_edges, scenario_junctions, 'AM', config, False, False)
    test.junctions.to_file('d:/test_junctions.shp', schema = {'geometry': 'Point','properties': {'is_zone': 'int', 'i' : 'int'}})
    link_schema = collections.OrderedDict({'geometry': 'LineString','properties': {'direction': 'int', 'i' : 'int', 'j' : 'int', 'length': 'float', 'modes' : 'str',  
                                                                                   'type' : 'int', 'lanes' : 'int', 'vdf' : 'int', 'ul1' : 'int', 'ul2' : 'int', 
                                                                                   'ul3' : 'int', 'PSRCEdgeID' : 'int'}})
    test.full_network.to_file('d:/test_network.shp', schema = link_schema)
    

    #test = CreateTransitSegmentTable(scenario_edges, gdf_TransitLines, gdf_TransitPoints, 'AM', config)
    #print 'done'
    
    # Do Trasit Stuff here
    gdf_TransitPoints['NewNodeID'] = gdf_TransitPoints.PSRCJunctI + 4000
    scenario_edges['weight'] = np.where(scenario_edges['FacilityTy'] == 99, .5 * scenario_edges.length, scenario_edges.length)
    
    route_id_list = gdf_TransitLines.LineID.tolist()

    pool = mp.Pool(12, build_transit_segments_parallel.init_pool, [scenario_edges, gdf_TransitLines, gdf_TransitPoints])
    results = pool.map(build_transit_segments_parallel.trace_transit_route, route_id_list)

    results = [item for sublist in results for item in sublist]
    pool.close()
   
    transit_segments = pd.DataFrame(results)
    transit_segments.to_csv('d:/test_transit_segments.csv')
    scenario_junctions.to_file('d:/scenario_junctions1.shp')
    scenario_edges.to_file('d:/scenario_edges1.shp')


    #scenario_edges.to_file(r'R:\Stefan\GDB_data\ScenarioEdges3.shp')

    end_time = datetime.datetime.now()
    elapsed_total = end_time - start_time
    logger.info('------------------------RUN ENDING_----------------------------------------------')
    logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))