import geopandas as gpd
import pandas as pd
import multiprocessing as mp
import networkx as nx
import log_controller

def trace_transit_route(route_id):
    #global global_edges
    #global global_transit_lines
    #global global_transit_points
    
    row_list = []
    line = global_transit_lines[global_transit_lines.LineID == route_id]
    points = global_transit_points[global_transit_points.LineID == route_id]
    points.sort_values('PointOrder', inplace = True)
    points_list = points['NewNodeID'].tolist()
    line['geometry'] = line.geometry.buffer(100)
    edges = gpd.sjoin(global_edges, line, how='inner',
                          op='within')
    x = nx.DiGraph()
    G = nx.from_pandas_edgelist(edges, 'i', 'j', ['weight'], x)
        
            
    stop_number = 1
    order = 1
    while len(points_list) > 1:
        try:    
            path = nx.shortest_path(G, points_list[0], points_list[1], 'weight')
            while len(path) > 1:
                row_list.append({'route_id' : int(route_id), 'INode' : int(path[0]), 'JNode' : int(path[1]), 'order' : order, 'stop_number' : stop_number})
                order = order + 1
                path.pop(0)
            points_list.pop(0)
            stop_number = stop_number + 1
        except:
            #edges.to_file('d:/edges' + str(route_id) + '.shp')
            logger = log_controller.logging.getLogger('main_logger')
            logger.info('No path between ' + str(points_list[0]) + 'and '+ str(points_list[1]) + ' in ' +  str(route_id))
            #global_logger.info("No path between %s and %s in route %s !" & (str(points_list[0]), str(points_list[1]), str(route_id)))
            row_list.append({'route_id' : route_id, 'INode' : points_list[0], 'JNode' : points_list[1], 'order' : 9999, 'stop_number' : 9999})
            print 'No path between ' + str(points_list[0]) + 'and '+ str(points_list[1]) + ' in ' +  str(route_id)
            break

    return row_list

def init_pool(edges, transit_lines, transit_points):
    global global_edges
    global_edges = edges
    global global_transit_lines
    global_transit_lines = transit_lines
    global global_transit_points
    global_transit_points = transit_points 

