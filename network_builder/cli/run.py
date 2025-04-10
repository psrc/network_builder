import pyogrio
import sys
import argparse
import pandas as pd
import numpy as np
import yaml
import datetime
import multiprocessing as mp
from pathlib import Path
from networkx.algorithms.components import *
import inro.emme.database.emmebank as _eb
import inro.emme.desktop.app as app
import json
import shutil
from rasterstats import zonal_stats, point_query
from shutil import copy2 as shcopy
import os
from network_builder.modules.emme_utilities import EmmeProject
from network_builder.modules.emme_utilities import EmmeNetwork
from network_builder.modules.validate_settings import ValidateSettings
from network_builder.modules.validate_settings import ValidateTableSettings
from network_builder.modules.FlagNetworkFromProjects import FlagNetworkFromProjects
from network_builder.modules.ThinNetwork import ThinNetwork
from network_builder.modules.hov_system import BuildHOVSystem
from network_builder.modules.BuildScenarioLinks import BuildScenarioLinks
from network_builder.modules.ConfigureTransitSegments import ConfigureTransitSegments
from network_builder.modules.file_system import FileSystem
from network_builder.modules.BuildZoneInputs import BuildZoneInputs
from network_builder.modules.TransitHeadways import TransitHeadways
from network_builder.modules.log_controller import *
from network_builder.modules.data_sources import NetworkData
from network_builder.modules.build_transit_segments_parallel import *
#try:
    # from ..modules.validate_settings import ValidateSettings
    # from ..modules.validate_settings import ValidateTableSettings
    #from .modules.log_controller import *
    #from .modules.data_sources import NetworkData
    #from ..modules.FlagNetworkFromProjects import FlagNetworkFromProjects
    #from .classes.ThinNetwork import ThinNetwork
    #from .classes.hov_system import BuildHOVSystem
    #from .classes.BuildScenarioLinks import BuildScenarioLinks
    #from .classes.ConfigureTransitSegments import ConfigureTransitSegments
    #from ..emme.emme_utilities import EmmeProject
    #from ..emme.emme_utilities import EmmeNetwork
    #from .classes.emme_utilities import create_bank
    #from .classes.BuildZoneInputs import BuildZoneInputs
    #from .classes.TransitHeadways import TransitHeadways
    #from .classes.file_system import FileSystem
    #from .build_transit_segments_parallel import *

#except ImportError:
    # from network_builder.modules.validate_settings import ValidateSettings
    # from network_builder.modules.validate_settings import ValidateTableSettings
    #from modules.log_controller import *
    #from modules.data_sources import NetworkData
    # from network_builder.modules.FlagNetworkFromProjects import FlagNetworkFromProjects
    # from classes.ThinNetwork import ThinNetwork
    # from classes.hov_system import BuildHOVSystem
    # from classes.BuildScenarioLinks import BuildScenarioLinks
    # from classes.ConfigureTransitSegments import ConfigureTransitSegments
    # #from network_builder.emme.emme_utilities import EmmeProject
    # #from network_builder.emme.emme_utilities import EmmeNetwork
    # from classes.BuildZoneInputs import BuildZoneInputs
    # from classes.TransitHeadways import TransitHeadways
    # from classes.file_system import FileSystem
    #from build_transit_segments_parallel import *


def add_run_args(parser, multiprocess=True):
    """
    Run command args
    """
    parser.add_argument(
        "-c",
        "--configs_dir",
        type=str,
        metavar="PATH",
        help="path to configs dir",
    )


def nodes_from_turns(turns, edges):
    edge_list = turns.FrEdgeID.tolist() + turns.ToEdgeID.tolist()
    edges = edges[edges.PSRCEdgeID.isin(edge_list)]
    return list(set(edges.INode.tolist() + edges.JNode.tolist()))


def nodes_from_transit(transit_points):
    return list(set(transit_points.PSRCJunctID.tolist()))


def nodes_from_centroids(junctions, config):
    centroid_junctions = junctions[junctions.PSRCjunctID <= config.max_zone_number]
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
    df = pd.DataFrame(pd.Series(node_list).value_counts())
    df.rename(columns={"count": "node_count"}, inplace=True)
    df = df[df.node_count == 2]
    return df.index.tolist()


def nodes_to_retain(edges, config, network_data):
    junctions = retain_junctions(network_data.gdf_Junctions)
    centroids = nodes_from_centroids(network_data.gdf_Junctions, config)
    turn_nodes = nodes_from_turns(network_data.gdf_TurnMovements, edges)
    transit_nodes = nodes_from_transit(network_data.gdf_TransitPoints)
    edge_nodes = nodes_from_edges(network_data.df_tolls["PSRCEdgeID"].tolist(), edges)
    return turn_nodes + centroids + transit_nodes + junctions + edge_nodes

def create_emme_bank(file_system, config):
    """Create an Emme bank and scenario."""
    emmebank_dimensions_dict = json.load(open("inputs/emme_bank_dimensions.json"))
    bank_path = Path(file_system.emme_dir/"emmebank")
    emmebank = _eb.create(bank_path, emmebank_dimensions_dict)
    emmebank.title = config.emmebank_title
    emmebank.unit_of_length = "mi"
    emmebank.coord_unit_length = 0.0001894
    scenario = emmebank.create_scenario(999)
    # project
    project = app.create_project(file_system.emme_dir, "emme_networks")
    desktop = app.start_dedicated(False, "SEC", project)
    data_explorer = desktop.data_explorer()
    database = data_explorer.add_database(bank_path)
    # open the database added so that there is an active one
    database.open()
    desktop.project.save()
    desktop.close()
    emme_toolbox_path = Path(f"{os.environ['EMMEPATH']}/toolboxes")
    shcopy(emme_toolbox_path/"standard.mtbx", file_system.emme_dir/"emme_networks")



def run(args):
    """
    Implements the 'run' sub-command, which
    runs network builder using the specified
    configs.
    """

    pd.options.display.float_format = "{:.4f}".format
    mp.freeze_support()
    build_network(args.configs_dir)
    sys.exit()


def build_network(configs_dir):

    start_time = datetime.datetime.now()
    config = yaml.safe_load(open(Path(f"{configs_dir}/config.yaml")))
    tables_config = yaml.safe_load(open(Path(f"{configs_dir}/tables_config.yaml")))
    config = ValidateSettings(**config)
    tables_config = ValidateTableSettings(**tables_config)

    file_system = FileSystem(config)
    

    # tart_transit_pool(1)

    
    logger = setup_custom_logger("main_logger", file_system, config)
    logger.info("Network Builder Started")
    logger.info(f"Configs Dir set to {configs_dir}")

    network_data = NetworkData(config, tables_config)
    logger.info("Finished data import")

    if config.output_crs:
        crs = config.output_crs
    else:
        crs = config.input_crs

    if config.update_network_from_projects:
        logger.info("Start updating network from projects")
        flagged_network = FlagNetworkFromProjects(
            network_data.gdf_TransRefEdges,
            network_data.gdf_ProjectRoutes,
            network_data.gdf_Junctions,
            config,
            logger
        )
        scenario_edges = flagged_network.scenario_edges
        logger.info("Finished updating network from projects")
    else:
        scenario_edges = network_data.gdf_TransRefEdges.loc[
            (
                (network_data.gdf_TransRefEdges.InServiceDate <= config.model_year)
                & (network_data.gdf_TransRefEdges.ActiveLink > 0)
                & (network_data.gdf_TransRefEdges.ActiveLink != 999)
            )
        ]
        scenario_edges["projRteID"] = 0

    logger.info("Start network thinning")
    start_edge_count = len(scenario_edges)
    retain_nodes = nodes_to_retain(scenario_edges, config, network_data)
    potential_thin_nodes = get_potential_thin_nodes(scenario_edges)
    potential_thin_nodes = [x for x in potential_thin_nodes if x not in retain_nodes]
    logger.info(" %s Potential nodes to thin", len(potential_thin_nodes))
    thinned_network = ThinNetwork(
        scenario_edges, network_data.gdf_Junctions, potential_thin_nodes, logger, config
    )
    scenario_edges = thinned_network.thinned_edges_gdf
    scenario_junctions = thinned_network.thinned_junctions_gdf
    final_edge_count = len(scenario_edges)
    logger.info(
        "Network went from %s edges to %s." % (start_edge_count, final_edge_count)
    )
    logger.info("Finished thinning network")

    # turns:
    turn_list = []
    for turn in network_data.gdf_TurnMovements.iterrows():
        turn = turn[1]
        j_node = turn.PSRCJunctID + config.node_offset
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
        turn_list.append(
            {
                "turn_id": turn.TurnID,
                "i_node": i_node,
                "j_node": j_node,
                "k_node": k_node,
            }
        )
    turn_df = pd.DataFrame(turn_list)
    turn_df = turn_df.merge(
        network_data.gdf_TurnMovements, how="left", left_on="turn_id", right_on="TurnID"
    )

    if config.create_emme_network:
        logger.info("creating emme bank")
        create_emme_bank(file_system, config)

        my_project = EmmeProject(
            file_system.emme_dir/"emme_networks/emme_networks.emp"
        )
        os.path.join(file_system.emme_dir/"emme_networks")

        
        # flag links that are HOV
        hov_columns = [col for col in scenario_edges.columns if "LanesHOV" in col]
        scenario_edges["is_hov"] = scenario_edges[hov_columns].sum(axis=1)
        scenario_edges["is_hov"] = np.where(scenario_edges["is_hov"] > 0, 1, 0)
        scenario_edges["is_managed"] = 0

        hov_system = BuildHOVSystem(scenario_edges, scenario_junctions, logger, config)

        bike_network = pd.DataFrame()

        for time_period in config.time_periods:
            dir = file_system.shapefile_dir/f"{time_period}"
            file_system.create_directory([dir])
        
            # get hov edges & junctions that are in this time period
            ij_field_name = "IJLanesHOV" + time_period
            ji_field_name = "JILanesHOV" + time_period
            # Get edges that have an hov attribute for this time period
            hov_edges = hov_system.hov_edges[
                (hov_system.hov_edges[ij_field_name] > 0)
                | (hov_system.hov_edges[ji_field_name] > 0)
            ]
            hov_junction_list = list(
                set(hov_edges.NewINode.tolist() + hov_edges.NewJNode.tolist())
            )
            hov_junctions = hov_system.hov_junctions[
                hov_system.hov_junctions.ScenarioNodeID.isin(hov_junction_list)
            ]

            hov_weave_edges = hov_system.hov_weave_edges[
                (hov_system.hov_weave_edges.NewINode.isin(hov_junction_list))
                | (hov_system.hov_weave_edges.NewJNode.isin(hov_junction_list))
            ]

            # test = BuildHOVSystem(scenario_edges, scenario_junctions, time_period, config)
            tod_edges = pd.concat([scenario_edges, pd.DataFrame(hov_weave_edges)])
            tod_edges = pd.concat(
                [tod_edges, pd.DataFrame(hov_edges)],
            )
            # need to reset so we dont have duplicate index values
            tod_edges.reset_index(inplace=True)
            tod_edges.crs = crs

            tod_junctions = pd.concat([scenario_junctions, hov_junctions])
            tod_junctions.reset_index(inplace=True)
            tod_junctions.crs = crs

            test = BuildScenarioLinks(
                tod_edges,
                tod_junctions,
                time_period,
                config,
                logger,
                config.reversibles[time_period][0],
                config.reversibles[time_period][1],
            )
            model_links = test.full_network
            model_nodes = test.junctions

            # Do Transit Stuff here
            network_data.gdf_TransitPoints["NewNodeID"] = (
                network_data.gdf_TransitPoints.PSRCJunctID + config.node_offset
            )
            model_links["weight"] = np.where(
                model_links["is_managed"] == 1,
                0.5 * model_links.length,
                model_links.length,
            )

            route_id_list = network_data.gdf_TransitLines.loc[
                network_data.gdf_TransitLines["Headway_" + time_period] > 0
            ].LineID.tolist()
            print(len(route_id_list))
            if route_id_list:
                logger.info("Start tracing %s routes", len(route_id_list))

                # when tracing, only use edges that support transit
                transit_edges = model_links.loc[
                    (model_links.i > config.max_zone_number)
                    & (model_links.j > config.max_zone_number)
                ].copy()
                transit_edges = transit_edges.loc[transit_edges["modes"] != "wk"]
                transit_edges = transit_edges.loc[transit_edges["lanes"] > 0]
                logger.info("number of pools is %s", config.number_of_pools)
                pool = mp.Pool(
                    config.number_of_pools,
                    init_transit_segment_pool,
                    [
                        transit_edges,
                        network_data.gdf_TransitLines,
                        network_data.gdf_TransitPoints,
                    ],
                )
                results = pool.map(
                    trace_transit_route,
                    route_id_list,
                )

                results = [item for sublist in results for item in sublist]
                #errors = [item for sublist in errors for item in sublist]
                # for error in errors:
                #     logger.warning(error)
                # logger.info("Finished tracing transit routes")

                pool.close()
                pool.join()

                transit_segments = pd.DataFrame(results)
                logger.info("transit segments %s", len(transit_segments))

                if len(transit_segments) > 1:
                    test = ConfigureTransitSegments(
                        time_period,
                        transit_segments,
                        network_data.gdf_TransitLines,
                        model_links,
                        config,
                        logger,
                    )
                    transit_segments = test.configure()
                else:
                    logger.warning(
                        "Warning: There are no transit segements to build transit routes!"
                    )

                if config.save_network_files:
                    transit_segments.to_csv(
                        os.path.join(dir, time_period + "_transit_segments.csv")
                    )

            # Use AM network to create zone, park and ride, and transit stops files
            if time_period == "AM":
                zonal_inputs = BuildZoneInputs(
                    model_nodes,
                    network_data.gdf_ProjectRoutes,
                    network_data.df_evtPointProjectOutcomes,
                    config,
                    logger,
                )
                zonal_inputs_tuple = zonal_inputs.build_zone_inputs()
                path = file_system.build_file_dir/"TAZIndex.txt"
                _df = zonal_inputs_tuple[0]
                _df.fillna(0, inplace=True)
                tazindex_cols = ["Zone_id", "zone_ordinal", "Dest_eligible", "External"]
                _df[tazindex_cols] = _df[tazindex_cols].astype("int32").astype("str")
                _df.to_csv(path, columns=tazindex_cols, index=False, sep="\t")
                path = file_system.build_file_dir/"p_r_nodes.csv"
                _df = zonal_inputs_tuple[1]
                _df[["NodeID", "ZoneID", "Capacity", "Cost"]] = (
                    _df[["NodeID", "ZoneID", "Capacity", "Cost"]]
                    .astype("int")
                    .astype("str")
                )
                _df.to_csv(
                    path,
                    columns=[
                        "NodeID",
                        "ZoneID",
                        "XCoord",
                        "YCoord",
                        "Capacity",
                        "Cost",
                    ],
                    index=False,
                )

                headways = TransitHeadways(
                    network_data.gdf_TransitLines,
                    network_data.df_transit_frequencies,
                    config,
                    logger,
                )
                headways_df = headways.build_headways()
                path =  file_system.build_file_dir/ "headways.csv"
                headways_df.to_csv(path)

                # Create transit stops file
                df = pd.DataFrame()
                for mode in pd.unique(network_data.gdf_TransitLines["Mode"]):
                    transit_edges_submode = network_data.gdf_TransitLines[
                        network_data.gdf_TransitLines["Mode"] == mode
                    ]
                    stops_df = network_data.gdf_TransitPoints[
                        network_data.gdf_TransitPoints["LineID"].isin(
                            transit_edges_submode["LineID"].values
                        )
                    ]
                    stops_df["submode"] = mode
                    stops_df["x"] = stops_df.geometry.x
                    stops_df["y"] = stops_df.geometry.y
                    df = pd.concat([df, stops_df[["submode", "x", "y", "PSRCJunctID"]]])
                # Now BRT
                transit_edges_submode = network_data.gdf_TransitLines[
                    network_data.gdf_TransitLines["TransitType"] == 3
                ]
                stops_df = network_data.gdf_TransitPoints[
                    network_data.gdf_TransitPoints["LineID"].isin(
                        transit_edges_submode["LineID"].values
                    )
                ]
                stops_df["submode"] = "z"
                stops_df["x"] = stops_df.geometry.x
                stops_df["y"] = stops_df.geometry.y
                df = pd.concat([df, stops_df[["submode", "x", "y", "PSRCJunctID"]]])

                # Now Street Car
                transit_edges_submode = network_data.gdf_TransitLines[
                    network_data.gdf_TransitLines["TransitType"] == 4
                ]
                stops_df = network_data.gdf_TransitPoints[
                    network_data.gdf_TransitPoints["LineID"].isin(
                        transit_edges_submode["LineID"].values
                    )
                ]
                stops_df["submode"] = "y"
                stops_df["x"] = stops_df.geometry.x
                stops_df["y"] = stops_df.geometry.y
                df = pd.concat([df, stops_df[["submode", "x", "y", "PSRCJunctID"]]])

                df = df.groupby(["submode", "PSRCJunctID"]).max().reset_index()
                for submode, colname in config.submode_dict.items():
                    df.loc[df["submode"] == submode, colname] = 1
                df.fillna(0, inplace=True)
                df.drop(columns=["submode"], inplace=True)
                # df = df.groupby('PSRCJunctID').max().reset_index()
                df.to_csv(file_system.build_file_dir/"transit_stops.csv", index=False
                )

            if config.build_bike_network:  # Only run this once
                # Only run this for one time period:
                if len(bike_network) == 0:
                    # Filter bikeable links (remove freeways, ramps, transit-only facilities, centroid connectors)
                    bike_network = model_links[
                        model_links["FacilityType"].isin(config.bike_facility_types)
                    ]

                    # Intersect elevation raster with all point features along each link
                    logger.info("Elevation raster start")
                    pts = point_query(bike_network, config.raster_file_path)
                    # pts = np.array(
                    #     point_query(bike_network, config["raster_file_path"])
                    # )
                    logger.info("Elevation raster done")
                    elev_dict = {}

                    for i in range(len(pts)):
                        id = bike_network.iloc[i].id
                        elev_dict[id] = np.array(pts[i])

                    # Calculate slope between points for all links
                    # Each link is composed of multiple points, depending on line geometry and length
                    # Slope is calculated in direction of link & only considers increases in slope
                    link_ids = bike_network["id"].tolist()
                    bike_pool = mp.Pool(
                        config.number_of_pools,
                        modules.build_bike_network_parallel.init_pool,
                        [bike_network, elev_dict, config],
                    )
                    avg_upslope = bike_pool.map(
                        modules.build_bike_network_parallel.calc_slope_parallel,
                        link_ids,
                    )

                    bike_pool.close()
                    bike_pool.join()

                    # Slope is the average increase in slope across the link (upslope)
                    bike_network["upslp"] = avg_upslope

                    logger.info("Bike work done")

                # Join slope to network
                model_links = model_links.merge(
                    bike_network[["i", "j", "upslp"]], on=["i", "j"], how="left"
                )
                model_links["upslp"] = model_links["upslp"].fillna(0)
            else:
                model_links["upslp"] = -1

            model_links.rename(columns={"id": "link_id"}, inplace=True)

            if config.save_network_files:
                # using pyogrio driver was causing an error here.
                # but need to use it earlier to import from file gdb.
                gpd.options.io_engine = "fiona"

                model_nodes.to_file(file_system.shapefile_dir/f"{time_period}_junctions.shp",
                    driver="ESRI Shapefile",
                )

                model_links.reset_index(drop=True, inplace=True)
                model_links.to_file(file_system.shapefile_dir/f"{time_period}_edges.shp",
                    driver="ESRI Shapefile",
                )
                

            if config.create_emme_network:
                if route_id_list:
                    model_nodes["invt"] = 1
                    model_nodes["wait"] = 2
                    model_nodes["hdwfr"] = 0.5

                    emme_network = EmmeNetwork(
                        my_project,
                        time_period,
                        network_data.gdf_TransitLines,
                        model_links,
                        model_nodes,
                        turn_df,
                        config,
                        logger,
                        transit_segments,
                    )
                else:
                    emme_network = EmmeNetwork(
                        my_project,
                        time_period,
                        network_data.gdf_TransitLines,
                        model_links,
                        model_nodes,
                        turn_df,
                        config,
                        logger,
                    )
                emme_network.load_network()

            if config.export_build_files:
                my_project.change_primary_scenario(time_period)

                path = file_system.roadway_dir/f"{time_period.lower()}_roadway.in"
                
                my_project.export_base_network(path)

                if route_id_list:
                    path = file_system.transit_dir/f"{time_period.lower()}_transit.in"
                    my_project.export_transit(path)

                path = file_system.turns_dir/f"{time_period.lower()}_turns.in"              
                my_project.export_turns(path)
                
                path = file_system.extra_attributes_dir/f"{time_period.lower()}_link_attributes.in"

                my_project.export_extra_attributes(["LINK"], path)
                my_project.export_extra_attributes(["NODE"], path)
                if route_id_list:
                    my_project.export_extra_attributes(["TRANSIT_LINE"], path)

                path = file_system.shape_dir/f"{time_period.lower()}_shape.in"
                my_project.export_shape(path)

    end_time = datetime.datetime.now()
    elapsed_total = end_time - start_time
    logger.info(
        "------------------------RUN ENDING_----------------------------------------------"
    )
    logger.info("TOTAL RUN TIME %s" % str(elapsed_total))

    print("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    add_run_args(parser)
    args = parser.parse_args()
    sys.exit(run(args))
