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


def nodes_from_turns(turns, edges):
    edge_list = turns.FrEdgeID.tolist() + turns.ToEdgeID.tolist()
    edges = edges[edges.PSRCEdgeID.isin(edge_list)]
    return list(set(edges.INode.tolist() + edges.JNode.tolist()))

def nodes_from_transit(transit_points):
    return list(set(transit_points.PSRCJunctI.tolist()))


def nodes_from_centroids(junctions):
    centroid_junctions = junctions[junctions.EMME2nodeI > 0]
    return centroid_junctions.PSRCjunctI.tolist()


def retain_junctions(junctions):
    retain_junctions = junctions[junctions.JunctionTy == 10]
    return retain_junctions.PSRCjunctI.tolist()


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

def nodes_to_retain(edges):
    turn_nodes = nodes_from_turns(gdf_TurnMovements, edges)
    centroids = nodes_from_centroids(gdf_Junctions)
    transit_nodes = nodes_from_transit(gdf_TransitPoints)
    junctions = retain_junctions(gdf_Junctions)
    return turn_nodes + centroids + transit_nodes + junctions
    



if __name__ == '__main__':
    config = yaml.safe_load(open("config.yaml"))

    logger = log_controller.setup_custom_logger('main_logger')
    logger.info('------------------------Network Builder Started----------------------------------------------')
    
    start_time = datetime.datetime.now()

    logger.info('Starting data import')
    from data_sources import *
    logger.info('Finished data import')

    model_year = config['model_year']

    scenario_edges = gdf_TransRefEdges.loc[((gdf_TransRefEdges.InServiceD <= config['model_year']) 
                                      & (gdf_TransRefEdges.ActiveLink > 0) 
                                      & (gdf_TransRefEdges.ActiveLink <> 999))]
    scenario_edges['projRteID'] = 0

    if config['update_network_from_projects']:
        logger.info('Starting updated network from projects')
        flagged_network = FlagNetworkFromProjects(gdf_TransRefEdges, gdf_ProjectRoutes, gdf_Junctions, config)
        scenario_edges = flagged_network.scenario_edges
        logger.info('Finished updating network from projects')

    
    logger.info('Starting network thinning')
    start_edge_count = len(scenario_edges)
    retain_nodes = nodes_to_retain(scenario_edges)
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
    for turn in gdf_TurnMovements.iterrows():
        turn = turn[1]
        j_node = turn.PSRCJunctI + config['node_offset']
        from_edge = scenario_edges[scenario_edges.PSRCEdgeID == turn.FrEdgeID]
        if from_edge.empty:
            logger.warning("Warning: From edge from Turn %s not found!" % (turn.TurnID))
            continue
        elif int(from_edge.NewINode) <> j_node:
            i_node = int(from_edge.NewINode)
        else:
            i_node = int(from_edge.NewJNode)
        to_edge = scenario_edges[scenario_edges.PSRCEdgeID == turn.ToEdgeID]
        if to_edge.empty:
            logger.warning("Warning: To edge from Turn %s not found!" % (turn.TurnID))
            continue
        elif int(to_edge.NewINode) <> j_node:
            k_node = int(to_edge.NewINode)
        else:
            k_node = int(to_edge.NewJNode)
        turn_list.append({'i_node' : i_node, 'j_node' : j_node, 'k_node' : k_node})
    turn_df = pd.DataFrame(turn_list)

    if config['create_emme_network']:
        logger.info("creating emme bank")
        emme_folder = os.path.join('outputs', config['emme_folder_name'])
        emmebank_dimensions_dict = json.load(open(os.path.join(config['data_path'], 'emme_bank_dimensions.json')))
        if os.path.exists(emme_folder):
            shutil.rmtree(emme_folder)
        os.makedirs(emme_folder)
        bank_path = os.path.join(emme_folder, 'emmebank')
        emmebank = _eb.create(bank_path, emmebank_dimensions_dict)
        emmebank.title = config['emmebank_title']
        emmebank.unit_of_length = 'mi'
        emmebank.coord_unit_length = 0.0001894  
        scenario = emmebank.create_scenario(999)
        # project
        project = app.create_project(emme_folder, 'emme_networks')
        desktop = app.start_dedicated(False, "SEC", project)
        data_explorer = desktop.data_explorer()   
        database = data_explorer.add_database(bank_path)
        #open the database added so that there is an active one
        database.open()
        desktop.project.save()
        desktop.close()
        emme_toolbox_path = os.path.join(os.environ['EMMEPATH'], 'toolboxes')
        shcopy(emme_toolbox_path + '/standard.mtbx', emme_folder + '\\emme_networks')
        my_project = EmmeProject(emme_folder + '\\emme_networks' + '\\emme_networks.emp')
        os.path.join(emme_folder + 'emme_networks')
        
        build_file_folder = os.path.join('outputs', config['emme_folder_name'], 'build_files')
        if os.path.exists(build_file_folder):
            shutil.rmtree(build_file_folder)
        os.makedirs(build_file_folder)
        os.makedirs(os.path.join(build_file_folder, 'roadway'))
        os.makedirs(os.path.join(build_file_folder, 'transit'))
        os.makedirs(os.path.join(build_file_folder, 'turns'))
        os.makedirs(os.path.join(build_file_folder, 'shape'))

        for time_period in config['time_periods']:
            dir = os.path.join('outputs', 'shapefiles', time_period)
            try:
                os.makedirs(dir)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
            test = BuildHOVSystem(scenario_edges, scenario_junctions, time_period, config)
            tod_edges = pd.concat([scenario_edges, pd.DataFrame(test.hov_weave_edges)])
            tod_edges = pd.concat([tod_edges, pd.DataFrame(test.hov_edges)],)
            # need to reset so we dont have duplicate index values
            tod_edges.reset_index(inplace = True)

            tod_junctions =  pd.concat([scenario_junctions, test.hov_junctions])
            tod_junctions.reset_index(inplace = True)

            test = BuildScenarioLinks(tod_edges, tod_junctions, time_period, config, config['reversibles'][time_period][0],config['reversibles'][time_period][1])
            model_links = test.full_network
            model_nodes = test.junctions
  
            # Do Trasit Stuff here
            gdf_TransitPoints['NewNodeID'] = gdf_TransitPoints.PSRCJunctI + config['node_offset']
            model_links['weight'] = np.where(model_links['FacilityTy'] == 99, .5 * model_links.length, model_links.length)
     
            # just do AM for now:
            route_id_list = gdf_TransitLines.loc[gdf_TransitLines['Headway_' + time_period] > 0].LineID.tolist()
            if route_id_list:
                logger.info("Start tracing %s routes", len(route_id_list))
  
                # when tracing, only use edges that support transit
                transit_edges = model_links.loc[(model_links.i > config['max_zone_number']) & (model_links.j > config['max_zone_number'])].copy()  
                transit_edges = transit_edges.loc[transit_edges['modes'] <> 'wk']
    
                pool = mp.Pool(12, build_transit_segments_parallel.init_pool, [transit_edges, gdf_TransitLines, gdf_TransitPoints])
                results = pool.map(build_transit_segments_parallel.trace_transit_route, route_id_list)

                results = [item for sublist in results for item in sublist]
                pool.close()

                transit_segments = pd.DataFrame(results)
                test = ConfigureTransitSegments(time_period, transit_segments, gdf_TransitLines, model_links, config)
                transit_segments = test.configure()
            
                if config['save_network_files'] :
                    transit_segments.to_csv(os.path.join(dir, time_period + '_transit_segments.csv'))
        
            if config['save_network_files'] :
                model_nodes.to_file(os.path.join(dir, time_period + '_junctions.shp'), schema = {'geometry': 'Point','properties': {'is_zone': 'int', 'i' : 'int'}})
                link_atts = collections.OrderedDict({'direction': 'int', 'i' : 'int', 'j' : 'int', 'length': 'float', 'modes' : 'str',  
                                                                                   'type' : 'int', 'lanes' : 'int', 'vdf' : 'int', 'ul1' : 'int', 
                                                                                   'ul2' : 'int', 'ul3' : 'int', 'PSRCEdgeID' : 'int', 
                                                                                   'FacilityTy' : 'int', 'weight' : 'float', 'id' : 'str', 'Processing_x' : 'int'})
                link_schema = collections.OrderedDict({'geometry': 'LineString','properties': link_atts})
                model_links.to_file(os.path.join(dir, time_period + '_edges.shp'), schema = link_schema)

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

               # path = os.path.join(build_file_folder, 'shape', time_period.lower() + '_shape.in')
               # my_project.export_shape(path)




    end_time = datetime.datetime.now()
    elapsed_total = end_time - start_time
    logger.info('------------------------RUN ENDING_----------------------------------------------')
    logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))