import time
import geopandas as gpd
import pandas as pd
import os
import sys
import errno
import numpy as np
from shapely.geometry import LineString
from shapely.geometry import Point
from rasterstats import zonal_stats, point_query
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
import build_bike_network_parallel
import collections
import inro.emme.database.emmebank as _eb
import inro.emme.desktop.app as app
import json
import shutil
from shutil import copy2 as shcopy
import multiprocessing as mp

def nodes_from_turns(turns, edges):
    edge_list = turns.FrEdgeID.tolist() + turns.ToEdgeID.tolist()
    edges = edges[edges.PSRCEdgeID.isin(edge_list)]
    return list(set(edges.INode.tolist() + edges.JNode.tolist()))

def nodes_from_transit(transit_points):
    return list(set(transit_points.PSRCJunctID.tolist()))


def nodes_from_centroids(junctions):
    centroid_junctions = junctions[junctions.EMME2nodeID > 0]
    return centroid_junctions.PSRCjunctID.tolist()

def retain_junctions(junctions):
    retain_junctions = junctions[junctions.JunctionType == 10]
    return retain_junctions.PSRCjunctID.tolist()


def nodes_from_edges(list_of_edges, edges):
    edges = edges[edges.PSRCEdgeID.isin(list_of_edges)]
    node_list = edges.INode.tolist()
    print(len(node_list))
    node_list = node_list + edges.JNode.tolist()
    return list(set(node_list))

def get_potential_thin_nodes(edges):
    node_list = edges.INode.tolist() + edges.JNode.tolist()
    df = pd.DataFrame(pd.Series(node_list).value_counts(), columns=['node_count'])
    df = df[df.node_count == 2]
    return df.index.tolist()

def nodes_to_retain(edges):
    junctions = retain_junctions(gdf_Junctions)
    centroids = nodes_from_centroids(gdf_Junctions)
    turn_nodes = nodes_from_turns(gdf_TurnMovements, edges)
    transit_nodes = nodes_from_transit(gdf_TransitPoints)
    edge_nodes = nodes_from_edges(df_tolls['PSRCEdgeID'].tolist(), edges)
    return turn_nodes + centroids + transit_nodes + junctions + edge_nodes


if __name__ == '__main__':
    pd.options.display.float_format = '{:.4f}'.format
    mp.freeze_support()

    config = yaml.safe_load(open("config.yaml"))

    logger = log_controller.setup_custom_logger('main_logger')
    logger.info('------------------------Network Builder Started----------------------------------------------')
    
    start_time = datetime.datetime.now()

    logger.info('Starting data import')
    from sql_data_sources import *
    logger.info('Finished data import')

    model_year = config['model_year']

    if config['update_network_from_projects']:
        logger.info('Start updating network from projects')
        flagged_network = FlagNetworkFromProjects(gdf_TransRefEdges, gdf_ProjectRoutes, gdf_Junctions, config)
        scenario_edges = flagged_network.scenario_edges
        logger.info('Finished updating network from projects')
    else:
         scenario_edges = gdf_TransRefEdges.loc[((gdf_TransRefEdges.InServiceDate <= config['model_year']) 
                                      & (gdf_TransRefEdges.ActiveLink > 0) 
                                      & (gdf_TransRefEdges.ActiveLink != 999))]
         scenario_edges['projRteID'] = 0

    logger.info('Start network thinning')
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
        j_node = turn.PSRCJunctID + config['node_offset']
        from_edge = scenario_edges[scenario_edges.PSRCEdgeID == turn.FrEdgeID]
        if from_edge.empty:
            logger.warning("Warning: From edge from Turn %s not found!" % (turn.TurnID))
            continue
        elif int(from_edge.NewINode) != j_node:
            i_node = int(from_edge.NewINode)
        else:
            i_node = int(from_edge.NewJNode)
        to_edge = scenario_edges[scenario_edges.PSRCEdgeID == turn.ToEdgeID]
        if to_edge.empty:
            logger.warning("Warning: To edge from Turn %s not found!" % (turn.TurnID))
            continue
        elif int(to_edge.NewINode) != j_node:
            k_node = int(to_edge.NewINode)
        else:
            k_node = int(to_edge.NewJNode)
        turn_list.append({'turn_id': turn.TurnID, 'i_node' : i_node, 'j_node' : j_node, 'k_node' : k_node})
    turn_df = pd.DataFrame(turn_list)
    turn_df = turn_df.merge(gdf_TurnMovements, how = 'left', left_on = 'turn_id', right_on = 'TurnID')

    if config['create_emme_network']:
        logger.info("creating emme bank")
        emme_folder = os.path.join(config['output_dir'], config['emme_folder_name'])
        emmebank_dimensions_dict = json.load(open('inputs/emme_bank_dimensions.json'))
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
        
        build_file_folder = os.path.join(config['output_dir'], config['emme_folder_name'], 'build_files')
        if os.path.exists(build_file_folder):
            shutil.rmtree(build_file_folder)
        os.makedirs(build_file_folder)
        os.makedirs(os.path.join(build_file_folder, 'roadway'))
        os.makedirs(os.path.join(build_file_folder, 'transit'))
        os.makedirs(os.path.join(build_file_folder, 'turns'))
        os.makedirs(os.path.join(build_file_folder, 'shape'))
        os.makedirs(os.path.join(build_file_folder, 'extra_attributes'))

        #flag links that are HOV
        hov_columns = [col for col in scenario_edges.columns if 'LanesHOV' in col]
        scenario_edges['is_hov'] = scenario_edges[hov_columns].sum(axis=1)
        scenario_edges['is_hov'] = np.where(scenario_edges['is_hov'] > 0, 1, 0)
        scenario_edges['is_managed'] = 0
        
        hov_system = BuildHOVSystem(scenario_edges, scenario_junctions, config)

        bike_network = pd.DataFrame()

        for time_period in config['time_periods']:
            dir = os.path.join(config['output_dir'], 'shapefiles', time_period)
            try:
                os.makedirs(dir)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
            # get hov edges & junctions that are in this time period
            ij_field_name = 'IJLanesHOV' + time_period
            ji_field_name = 'JILanesHOV' + time_period
            # Get edges that have an hov attribute for this time period
            hov_edges = hov_system.hov_edges[(hov_system.hov_edges[ij_field_name] >
                                      0) | (hov_system.hov_edges
                                            [ji_field_name] > 0)]
            hov_junction_list = list(set(hov_edges.NewINode.tolist() + hov_edges.NewJNode.tolist()))
            hov_junctions = hov_system.hov_junctions[hov_system.hov_junctions.ScenarioNodeID.isin(hov_junction_list)]

            hov_weave_edges = hov_system.hov_weave_edges[(hov_system.hov_weave_edges.NewINode.isin(hov_junction_list)) | (hov_system.hov_weave_edges.NewJNode.isin(hov_junction_list))]

            #test = BuildHOVSystem(scenario_edges, scenario_junctions, time_period, config)
            tod_edges = pd.concat([scenario_edges, pd.DataFrame(hov_weave_edges)])
            tod_edges = pd.concat([tod_edges, pd.DataFrame(hov_edges)],)
            # need to reset so we dont have duplicate index values
            tod_edges.reset_index(inplace = True)
            tod_edges.crs =  {'init' : 'EPSG:2285'}

            tod_junctions =  pd.concat([scenario_junctions, hov_junctions])
            tod_junctions.reset_index(inplace = True)
            tod_junctions.crs =  {'init' : 'EPSG:2285'}

            test = BuildScenarioLinks(tod_edges, tod_junctions, time_period, config, config['reversibles'][time_period][0],config['reversibles'][time_period][1])
            model_links = test.full_network
            model_nodes = test.junctions

            # Do Transit Stuff here
            gdf_TransitPoints['NewNodeID'] = gdf_TransitPoints.PSRCJunctID + config['node_offset']
            model_links['weight'] = np.where(model_links['is_managed'] == 1, .5 * model_links.length, model_links.length)
     
            route_id_list = gdf_TransitLines.loc[gdf_TransitLines['Headway_' + time_period] > 0].LineID.tolist()
            print (len(route_id_list))
            if route_id_list:
                logger.info("Start tracing %s routes", len(route_id_list))
  
                # when tracing, only use edges that support transit
                transit_edges = model_links.loc[(model_links.i > config['max_zone_number']) & (model_links.j > config['max_zone_number'])].copy()  
                transit_edges = transit_edges.loc[transit_edges['modes'] != 'wk']
                transit_edges = transit_edges.loc[transit_edges['lanes'] > 0]
    
                pool = mp.Pool(config['number_of_pools'], build_transit_segments_parallel.init_pool, [transit_edges,gdf_TransitLines, gdf_TransitPoints])
                results = pool.map(build_transit_segments_parallel.trace_transit_route, route_id_list)
                

                results = [item for sublist in results for item in sublist]
                pool.close()
                pool.join()

                transit_segments = pd.DataFrame(results)
                if len(transit_segments) > 1:
                    test = ConfigureTransitSegments(time_period, transit_segments, gdf_TransitLines, model_links, config)
                    transit_segments = test.configure()
                else:
                    logger.warning("Warning: There are no transit segements to build transit routes!")
            
                if config['save_network_files'] :
                    transit_segments.to_csv(os.path.join(dir, time_period + '_transit_segments.csv'))

            # Use AM network to create zone, park and ride, and transit stops files   
            if time_period == 'AM':

                zonal_inputs = BuildZoneInputs(model_nodes, gdf_ProjectRoutes, df_evtPointProjectOutcomes, config)
                zonal_inputs_tuple = zonal_inputs.build_zone_inputs()
                path = os.path.join(build_file_folder, 'TAZIndex.txt')
                _df = zonal_inputs_tuple[0]
                _df.fillna(0, inplace=True)
                tazindex_cols = ['Zone_id', 'zone_ordinal', 'Dest_eligible', 'External']
                _df[tazindex_cols] = _df[tazindex_cols].astype('int32').astype('str')
                _df.to_csv(path, columns = tazindex_cols, index=False, sep='\t')
                path = os.path.join(build_file_folder, 'p_r_nodes.csv')
                _df = zonal_inputs_tuple[1]
                _df[['NodeID','ZoneID','Capacity','Cost']] = _df[['NodeID','ZoneID','Capacity','Cost']].astype('int').astype('str')
                _df.to_csv(path, columns = ['NodeID', 'ZoneID', 'XCoord', 'YCoord', 'Capacity', 'Cost'], index=False) 
                
                headways = TransitHeadways(gdf_TransitLines, df_transit_frequencies, config)
                headways_df = headways.build_headways()
                path = os.path.join(build_file_folder, 'headways.csv')
                headways_df.to_csv(path)

                # Create transit stops file
                df = pd.DataFrame()
                for mode in pd.unique(gdf_TransitLines['Mode']):
                    transit_edges_submode = gdf_TransitLines[gdf_TransitLines['Mode'] == mode]
                    stops_df = gdf_TransitPoints[gdf_TransitPoints['LineID'].isin(transit_edges_submode['LineID'].values)]
                    stops_df['submode'] = mode
                    stops_df['x'] = stops_df.geometry.x
                    stops_df['y'] = stops_df.geometry.y
                    df = df.append(stops_df[['submode','x','y','PSRCJunctID']])

                df = df.groupby(['submode','PSRCJunctID']).max().reset_index()
                for submode, colname in config['submode_dict'].items(): 
                    df.loc[df['submode'] == submode, colname] = 1
                df.fillna(0, inplace=True)
                df.drop('submode', axis=1, inplace=True)
                df.to_csv(os.path.join(build_file_folder,'transit_stops.csv'), index=False)
            
            if config['build_bike_network']:    # Only run this once

                # Only run this for one time period:
                if len(bike_network) == 0:
                    
                    # Filter bikeable links (remove freeways, ramps, transit-only facilities, centroid connectors)
                    bike_network = model_links[model_links['FacilityType'].isin(config['bike_facility_types'])]

                    # Intersect elevation raster with all point features along each link
                    logger.info('Elevation raster start')
                    pts = np.array(point_query(bike_network, config['raster_file_path']))
                    logger.info('Elevation raster done')
                    elev_dict = {}

                    for i in range(len(pts)):
                        id = bike_network.iloc[i].id
                        elev_dict[id] = pts[i]

                    # Calculate slope between points for all links
                    # Each link is composed of multiple points, depending on line geometry and length
                    # Slope is calculated in direction of link & only considers increases in slope
                    link_ids = bike_network['id'].tolist()
                    bike_pool = mp.Pool(config['number_of_pools'], build_bike_network_parallel.init_pool, 
                                    [bike_network, elev_dict])
                    avg_upslope = bike_pool.map(build_bike_network_parallel.calc_slope_parallel, link_ids)

                    bike_pool.close()
                    bike_pool.join()  

                    # Slope is the average increase in slope across the link (upslope)
                    bike_network['upslp'] = avg_upslope

                    logger.info('Bike work done')

                # Join slope to network
                model_links = model_links.merge(bike_network[['i','j','upslp']], on=['i','j'], how='left')
                model_links['upslp'] = model_links['upslp'].fillna(0)
            else:
                 model_links['upslp'] = -1

            if config['save_network_files'] :
                model_nodes.to_file(os.path.join(dir, time_period + '_junctions.shp'), driver='ESRI Shapefile')

                model_links.reset_index(drop = True, inplace = True)
                model_links.to_file(os.path.join(dir, time_period + '_edges.shp'),  driver='ESRI Shapefile', Index =  False)

            if config['create_emme_network']:
                if route_id_list:
   
                    model_nodes['invt'] = 1
                    model_nodes['wait'] = 2
                    model_nodes['hdwfr'] = .5

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