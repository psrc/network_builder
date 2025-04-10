import geopandas as gpd
import pandas as pd
import multiprocessing as mp
import networkx as nx
from network_builder.modules.log_controller import *
# try:
#     from .modules.log_controller import *
# except ImportError:
#     from modules.log_controller import *

def trace_transit_route(route_id):
    
    row_list = []
    line = global_transit_lines[global_transit_lines.LineID == route_id]
    points = global_transit_points[global_transit_points.LineID == route_id]
    points.sort_values("PointOrder", inplace=True)
    points_list = points["NewNodeID"].tolist()
    line["geometry"] = line.geometry.buffer(100)
    edges = global_edges[["geometry", "i", "j", "weight"]]
    edges = gpd.sjoin(edges, line, how="inner", predicate="within")
    x = nx.DiGraph()
    G = nx.from_pandas_edgelist(edges, "i", "j", ["weight"], x)

    stop_number = 1
    order = 1
    while len(points_list) > 1:
        try:
            path = nx.shortest_path(G, points_list[0], points_list[1], "weight")
            while len(path) > 1:
                row_list.append(
                    {
                        "route_id": int(route_id),
                        "INode": int(path[0]),
                        "JNode": int(path[1]),
                        "order": order,
                        "stop_number": stop_number,
                    }
                )
                order = order + 1
                path.pop(0)
            points_list.pop(0)
            stop_number = stop_number + 1
        except Exception:
            logger = logging.getLogger('main_logger')
            logger.info('No path between ' + str(points_list[0]) + 'and '+ str(points_list[1]) + ' in ' +  str(route_id))

            row_list.append(
                {
                    "route_id": route_id,
                    "INode": points_list[0],
                    "JNode": points_list[1],
                    "order": 9999,
                    "stop_number": 9999,
                }
            )
            
            break

    return row_list


def init_transit_segment_pool(edges, transit_lines, transit_points):
    global global_edges
    global_edges = edges
    global global_transit_lines
    global_transit_lines = transit_lines
    global global_transit_points
    global_transit_points = transit_points
