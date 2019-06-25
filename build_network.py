import time
import geopandas as gpd
import pandas as pd
import os
import sys
import errno
import numpy as np
from shapely.geometry import LineString
from shapely.geometry import Point
import yaml
from FlagNetworkFromProjects import *
from ThinNetwork import *
from BuildHOVSystem import *
from BuildScenarioLinks import *
from ConfigureTransitSegments import *
from EmmeProject import *
from EmmeNetwork import *
from BuildZoneInputs import *
from TransitHeadways import *
import logging
import log_controller
import datetime
import networkx as nx
from networkx.algorithms.components import *
import collections
import multiprocessing as mp
import build_transit_segments_parallel
import collections
import inro.emme.database.emmebank as _eb
import inro.emme.desktop.app as app
import json
import shutil
from shutil import copy2 as shcopy


def nodes_from_turns(turns, edges, network_config):
    edge_list = turns[network_config['turns']['from_link_id']].tolist() + turns[network_config['turns']['to_link_id']].tolist()
    edges = edges[edges[network_config['edges']['id']].isin(edge_list)]
    return list(set(edges.INode.tolist() + edges.JNode.tolist()))    # Should this be INode and JNode or NewINode?

def nodes_from_transit(transit_points, network_config):
    return list(set(transit_points[network_config['transit_points']['id']].tolist()))

def nodes_from_centroids(junctions, network_config):
    centroid_junctions = junctions[junctions[network_config['junctions']['id']] > 0]
    return centroid_junctions[network_config['junctions']['node']].tolist()

def retain_junctions(junctions, network_config):
    retain_junctions = junctions[junctions[network_config['junctions']['type']] == 10]
    return retain_junctions[network_config['junctions']['id']].tolist()

def nodes_from_edges(list_of_edges, edges, network_config):
    edges = edges[edges[network_config['edges']['id']].isin(list_of_edges)]

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

def nodes_to_retain(edges, network_config):
    junctions = retain_junctions(gdf_Junctions, network_config)
    centroids = nodes_from_centroids(gdf_Junctions, network_config)
    turn_nodes = nodes_from_turns(gdf_TurnMovements, edges, network_config)
    transit_nodes = nodes_from_transit(gdf_TransitPoints, network_config)
    edge_nodes = nodes_from_edges(df_tolls[network_config['edges']['id']].tolist(), edges)
    return turn_nodes + centroids + transit_nodes + junctions + edge_nodes
    

if __name__ == '__main__':

    pd.options.display.float_format = '{:.4f}'.format

    config = yaml.safe_load(open("config.yaml"))
    network_config = yaml.safe_load(open("network_config.yaml"))
    edges_cols = network_config['edges']

    logger = log_controller.setup_custom_logger('main_logger')
    logger.info('------------------------Network Builder Started----------------------------------------------')
	    
    start_time = datetime.datetime.now()

    logger.info('Starting data import')
    from sql_data_sources import *
    logger.info('Finished data import')

    model_year = config['model_year']

    #scenario_edges = gdf_TransRefEdges.loc[((gdf_TransRefEdges[edges_cols['project_start_year']] <= config['model_year']) 
	   #                                 & (gdf_TransRefEdges[edges_cols['active_link_flag']] > 0) 
	   #                                 & (gdf_TransRefEdges[edges_cols['active_link_flag']] != 999))]
    #scenario_edges[edges_cols['project_id']] = 0

    if config['update_network_from_projects']:
	    logger.info('Start updating network from projects')
	    flagged_network = FlagNetworkFromProjects(gdf_TransRefEdges, gdf_ProjectRoutes, gdf_Junctions, config)
	    scenario_edges = flagged_network.scenario_edges
	    logger.info('Finished updating network from projects')
    else:
	    scenario_edges = gdf_TransRefEdges.loc[((gdf_TransRefEdges[edges_cols['project_start_year']] <= config['model_year']) 
	                                & (gdf_TransRefEdges[edges_cols['active_link_flag']] > 0) 
	                                & (gdf_TransRefEdges[edges_cols['active_link_flag']] != 999))]
	    scenario_edges[network_config['edges']['project_id']] = 0

    logger.info('Start network thinning')
    start_edge_count = len(scenario_edges)
    retain_nodes = nodes_to_retain(scenario_edges, network_config)
    potential_thin_nodes = get_potential_thin_nodes(scenario_edges)
    potential_thin_nodes = [x for x in potential_thin_nodes if x not in retain_nodes]
    logger.info(" %s Potential nodes to thin", len(potential_thin_nodes))
    thinned_network = ThinNetwork(scenario_edges, gdf_Junctions, potential_thin_nodes, config)
    scenario_edges = thinned_network.thinned_edges_gdf
    scenario_junctions = thinned_network.thinned_junctions_gdf
    final_edge_count = len(scenario_edges)
    logger.info("Network went from %s edges to %s." % (start_edge_count, final_edge_count))
    logger.info("Finished thinning network")

    #turns:
    turn_list = []
    turn_cols = network_config['turns']
    for turn in gdf_TurnMovements.iterrows():
	    turn = turn[1]
	    j_node = turn[turn_cols['junction']] + config['node_offset']
	    from_edge = scenario_edges[scenario_edges[network_config['edges']['id']] == turn[turn_cols['from_link_id']]]
	    if from_edge.empty:
	        logger.warning("Warning: From edge from Turn %s not found!" % (turn[turn_cols['id']]))
	        continue
	    elif int(from_edge[network_config['edges']['i_node']]) <> j_node:
	        i_node = int(from_edge[network_config['edges']['i_node']])
	    else:
	        i_node = int(from_edge[network_config['edges']['j_node']])
	    to_edge = scenario_edges[scenario_edges[network_config['edges']['id']] == turn[turn_cols['to_link_id']]]
	    if to_edge.empty:
	        logger.warning("Warning: To edge from Turn %s not found!" % (turn[turn_cols['id']]))
	        continue
	    elif int(to_edge[network_config['edges']['i_node']]) <> j_node:
	        k_node = int(to_edge[network_config['edges']['i_node']])
	    else:
	        k_node = int(to_edge[network_config['edges']['j_node']])
	    turn_list.append({'turn_id': turn[turn_cols['id']], 'i_node' : i_node, 'j_node' : j_node, 'k_node' : k_node})
    turn_df = pd.DataFrame(turn_list)
    turn_df = turn_df.merge(gdf_TurnMovements, how = 'left', left_on = 'turn_id', right_on = 'TurnID')

    if config['create_emme_network']:
	    logger.info("creating emme bank")
	    emme_folder = os.path.join(config['output_dir'], config['emme_folder_name'])
	    emmebank_dimensions_dict = json.load(open(os.path.join(config['data_path'], 'emme_bank_dimensions.json')))
	    if os.path.exists(emme_folder):
	        shutil.rmtree(emme_folder)
	    os.makedirs(emme_folder)
	    bank_path = os.path.join(emme_folder, 'emmebank')
	    emmebank = _eb.create(bank_path, emmebank_dimensions_dict)
	    emmebank.title = config['emmebank_title']
	    emmebank.unit_of_length = config['unit_of_length']
	    emmebank.coord_unit_length = config['coord_unit_length']
	    scenario = emmebank.create_scenario(999)
	    # project
	    project = app.create_project(emme_folder, 'emme_networks')
	    desktop = app.start_dedicated(False, "SEC", project)
	    data_explorer = desktop.data_explorer()   
	    database = data_explorer.add_database(bank_path)
	    #open the database  so that there is an active one
	    database.open()
	    desktop.project.save()
	    desktop.close()
	    emme_toolbox_path = os.path.join(os.environ['EMMEPATH'], 'toolboxes')
	    shcopy(emme_toolbox_path + '/standard.mtbx', emme_folder + '\\emme_networks')
	    my_project = EmmeProject(emme_folder + '\\emme_networks' + '\\emme_networks.emp')
	    os.path.join(emme_folder + 'emme_networks')
	        
	    build_file_folder = os.path.join(config['output_dir'], config['emme_folder_name'], 'build_files')
	    if os.path.exists(build_file_folder):
	        shutil.rmtree(build_file_folder)
	    os.makedirs(build_file_folder)
	    os.makedirs(os.path.join(build_file_folder, 'roadway'))
	    os.makedirs(os.path.join(build_file_folder, 'transit'))
	    os.makedirs(os.path.join(build_file_folder, 'turns'))
	    os.makedirs(os.path.join(build_file_folder, 'shape'))
	    os.makedirs(os.path.join(build_file_folder, 'extra_attributes'))

	    for time_period in config['time_periods']:
	        dir = os.path.join(config['output_dir'], 'shapefiles', time_period)
	        try:
	            os.makedirs(dir)
	        except OSError as e:
	            if e.errno != errno.EEXIST:
	                raise
	        hov_network = BuildHOVSystem(scenario_edges, scenario_junctions, time_period, config)
	        tod_edges = pd.concat([scenario_edges, pd.DataFrame(hov_network.hov_weave_edges)])
	        tod_edges = pd.concat([tod_edges, pd.DataFrame(hov_network.hov_edges)],)
	        # need to reset so we dont have duplicate index values
	        tod_edges.reset_index(inplace = True)
	        tod_edges.crs =  {'init' : config['crs']['init']}

	        tod_junctions =  pd.concat([scenario_junctions, hov_network.hov_junctions])
	        tod_junctions.reset_index(inplace = True)
	        tod_junctions.crs =  {'init' : config['crs']['init']}

	        scenario_links = BuildScenarioLinks(tod_edges, tod_junctions, time_period, config, config['reversibles'][time_period][0],config['reversibles'][time_period][1])
	        model_links = scenario_links.full_network
	        model_nodes = scenario_links.junctions

	        # Use AM network to create zone, park and ride files   
	        if time_period == 'AM':
	            zonal_inputs = BuildZoneInputs(model_nodes, gdf_ProjectRoutes, df_evtPointProjectOutcomes, config)
	            zonal_inputs_tuple = zonal_inputs.build_zone_inputs()
	            path = os.path.join(build_file_folder, 'TAZIndex.txt')
	            zonal_inputs_tuple[0].to_csv(path, columns = config['taz_index_columns'], index = False, sep='\t')
	            path = os.path.join(build_file_folder, 'p_r_nodes.csv')
	            zonal_inputs_tuple[1].to_csv(path, columns = config['park_and_ride_columns'], index = False) 


	            headways = TransitHeadways(gdf_TransitLines, df_transit_frequencies, config)
	            headways_df = headways.build_headways()
	            path = os.path.join(build_file_folder, 'headways.csv')
	            headways_df.to_csv(path)

	        # Build Transit Segments and Lines
	        transit_cols = network_config['transit_points']
	        gdf_TransitPoints['newNodeID'] = gdf_TransitPoints[transit_cols['id']] + config['node_offset']
	        model_links['weight'] = np.where(model_links[network_config['edges']['facility_type']] == 999, 0.5 * model_links.length, model_links.length)
	     
	        route_id_list = gdf_TransitLines.loc[gdf_TransitLines['Headway_' + time_period] > 0].LineID.tolist()
	        if route_id_list:
	            logger.info("Start tracing %s routes", len(route_id_list))
	  
	            # when tracing, only use edges that support transit
	            transit_edges = model_links.loc[(model_links.i > config['max_zone_number']) & (model_links.j > config['max_zone_number'])].copy()  
	            transit_edges = transit_edges.loc[transit_edges['modes'] != 'wk']
	            transit_edges = transit_edges.loc[transit_edges['lanes'] > 0]
	    
	            pool = mp.Pool(config['number_of_pools'], build_transit_segments_parallel.init_pool, [transit_edges, gdf_TransitLines, gdf_TransitPoints])
	            results = pool.map(build_transit_segments_parallel.trace_transit_route, route_id_list)

	            results = [item for sublist in results for item in sublist]
	            pool.close()

	            transit_segments = pd.DataFrame(results)
	            if len(transit_segments) > 1:
	                transit_segments = ConfigureTransitSegments(time_period, transit_segments, gdf_TransitLines, model_links, config)
	                transit_segments = transit_segments.configure()
	            else:
	                logger.warning("Warning: There are no transit segements to build transit routes!")
	            
	            if config['save_network_files'] :
	                transit_segments.to_csv(os.path.join(dir, time_period + '_transit_segments.csv'))
	        
	        if config['save_network_files'] :
	            model_nodes.to_file(os.path.join(dir, time_period + '_junctions.shp'), driver='ESRI Shapefile')
	            model_links.to_file(os.path.join(dir, time_period + '_edges.shp'),  driver='ESRI Shapefile')

	        if config['create_emme_network']:
	            if route_id_list:
	                emme_network = EmmeNetwork(my_project, time_period, gdf_TransitLines, model_links, model_nodes, turn_df, config, transit_segments)
	            else:
	                emme_network = EmmeNetwork(my_project, time_period, gdf_TransitLines, model_links, model_nodes, turn_df, config)
	            emme_network.load_network()
	                

	        if config['export_build_files']:
	            my_project.change_primary_scenario(time_period)
	                
	            path = os.path.join(build_file_folder, 'roadway', time_period.lower() + '_roadway.in')
	            my_project.export_base_network(path)

	            if route_id_list:
	                path = os.path.join(build_file_folder, 'transit', time_period.lower() + '_transit.in')
	                my_project.export_transit(path)
	                
	            path = os.path.join(build_file_folder, 'turns', time_period.lower() + '_turns.in')
	            my_project.export_turns(path)

	            path = os.path.join(build_file_folder, 'extra_attributes', time_period.lower() + '_link_attributes.in')
	            my_project.export_extra_attributes(['LINK'], path)
	            my_project.export_extra_attributes(['NODE'], path)

	            path = os.path.join(build_file_folder, 'shape', time_period.lower() + '_shape.in')
	            my_project.export_shape(path)

    end_time = datetime.datetime.now()
    elapsed_total = end_time - start_time
    logger.info('------------------------RUN ENDING_----------------------------------------------')
    logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))