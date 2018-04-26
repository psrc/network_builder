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

# Join project attributes to project edges:
#project_edges =  project_edges.merge(df_tblLineProjects, on = 'projRteID', how = 'left')

## Get the edges used in the model

#scenario_edges = gdf_TransRefEdges[(gdf_TransRefEdges.InServiceD <= model_year) 
#                                      & (gdf_TransRefEdges.ActiveLink > 0) 
#                                      & (gdf_TransRefEdges.ActiveLink <> 999)]


#ij_projects = project_edges[project_edges.dir == "IJ"].copy()
#ji_projects =  project_edges[project_edges.dir == "JI"].copy()
#ij_columns = [x for x in ij_projects if x[0:2] == "IJ" or x[0:2] == "JI" ]
#switch_columns = [x[1] + x[0] + x[2:] for x in ij_columns]
#rename_dict = dict(zip(ij_columns, switch_columns))
#ji_projects = ji_projects.rename(columns = rename_dict)

#gdf_TransRefEdges['projRteID'] = 0
#merged_projects = pd.concat([ji_projects, ij_projects])
#merged_projects = merged_projects[['PSRCEdgeID', 'projRteID', 'JILanesGPAM', 'IJLanesGPAM']]
#gdf_TransRefEdges.set_index('PSRCEdgeID', inplace = True)
#merged_projects.set_index('PSRCEdgeID', inplace = True)
#merged_projects['IJLanesGPMD'] = 10
#merged_projects.replace(-1, np.NaN, inplace = True)
#gdf_TransRefEdges.update(merged_projects)
#gdf_TransRefEdges.reset_index(inplace = True)




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

#def compare_attributes(row1, row2, row2_dir= 'IJ'):
#    df1 = pd.DataFrame(row1).T
#    df2 = pd.DataFrame(row2).T
#    ij_cols = ['FacilityTy', 'Modes', 'Oneway', 'CountID', 'CountyID', 'IJLanesGPAM', 'JILanesGPAM']
#    ji_cols = ['FacilityTy', 'Modes', 'Oneway', 'CountID', 'CountyID', 'JILanesGPAM', 'IJLanesGPAM']
#    if row2_dir == 'IJ' and len(pd.merge(df1, df2, on=ij_cols)) > 0:
#        return True
#    elif row2_dir == 'JI' and len(pd.merge(df1, df2, right_on=ij_cols, left_on=ji_cols)) > 0:
#        return True
#    else:
#        return False

#def find_sub_list(sl,l):
#    results=[]
#    sll=len(sl)
#    for ind in (i for i,e in enumerate(l) if e==sl[0]):
#        if l[ind:ind+sll]==sl:
#            results.append((ind,ind+sll-1))
#    if len(results) == 0:
#        results.append(-1)
#    return results


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


#df = scenario_edges[scenario_edges.INode.isin(potential_thin_nodes) | scenario_edges.JNode.isin(potential_thin_nodes)]
# first get all the directional columns
#ij_cols = [x for x in scenario_edges if x[0:2] == "IJ" or x[0:2] == "JI" ]
## now add any other needed cols:
#cols = ['PSRCEdgeID', 'FacilityTy', 'Modes', 'INode', 'JNode', 'Oneway', 'CountID', 'CountyID', 'geometry'] + ij_cols
#G = nx.from_pandas_edgelist(scenario_edges, 'INode', 'JNode', cols)
#f = nx.Graph()                                                                                                                                     
#fedges = filter(lambda x: G.degree()[x[0]] == 2 and G.degree()[x[1]]==2, G.edges)
## get rid of any edge that has a node that should not be thinned
#fedges = [x for x in fedges if (x[0] not in no_thin_node_list) and (x[1] not in no_thin_node_list)]
## remove edges with 
#f.add_edges_from(fedges)
#connected = connected_components(f)
#seg_id = 1
#edge_list =[]
#for seg in connected:
#    if seg_id % 100 == 0:
#                print("%d Nodes Processed" % (seg_id))
#    order = 1
#    seg = list(seg)
#    start_end_list = [x for x in seg if f.degree(x) == 1]
#    path = nx.all_simple_paths(f, start_end_list[0], start_end_list[1]).next()
#    #start_tuple = (path[0], path[1])
#    #end_tuple = (path[-2], path[-1])
#    # find the connecting edges to path that were removed (these can be thinned as well)
#    first_segs = [x[0] for x in G.edges(start_end_list[0])] + [x[1] for x in G.edges(start_end_list[0])]
#    first_node = [x for x in first_segs if x not in path]
#    if first_node:
#        if first_node[0] not in no_thin_node_list:
#            path = first_node + path 
#    last_segs = [x[0] for x in G.edges(start_end_list[1])] + [x[1] for x in G.edges(start_end_list[1])]
#    last_node = [x for x in last_segs if x not in path]
#    if last_node:
#        if last_node[0] not in no_thin_node_list:
#            path = path + last_node
#    # get the dir of the first edge, will to compare directionality for rest of edges
#    if path[0] == G.get_edge_data(path[0], path[1])['INode']:
#        path_dir = 'IJ'
#    else:
#        path_dir = 'JI'

#    prev_merge = False
#    for y in range(0, len(path)):
#        # only 1 line
#        if len(path) == 2:
#            edge1 = G.get_edge_data(path[y], path[y+1])
#            edge_list.append(edge1)
#            break
#        # the last merge included the last edge
#        elif prev_merge and len(path) - 2 == y:
#            if path_dir == 'IJ':
#                edge1['JNode'] = path[y+1]
#            else:
#                edge1['JNode'] = path[y+1]

#            edge_list.append(edge1)
#            break
#        # if there are two left and prev_merged == False,
#        # means the last edge did not get mreged. Add it now and
#        # eave loop
#        elif len(path) - 2 == y:
#            edge1 = G.get_edge_data(path[y], path[y+1])
#            edge_list.append(edge1)
#            if path_dir == 'IJ':
#                edge1['JNode'] = path[y+1]
#            else:
#                edge1['JNode'] = path[y+1]

#            edge_list.append(edge1)
#            break
#        # end of the line 
#        elif y == len(path) - 1:
#            if path_dir == 'IJ':
#                edge1['JNode'] = path[y+1]
#            else:
#                edge1['JNode'] = path[y+1]

#            edge_list.append(edge1)
#        else:
#            non_dir_cols = ['FacilityTy', 'Modes', 'Oneway', 'CountID', 'CountyID'] 
#            compare_cols = non_dir_cols + ij_cols
#            if not prev_merge:
#                #get the first and second edge from the path sequence
#                edge1 = G.get_edge_data(path[y], path[y+1])
#                if edge1['INode'] == path[y]:
#                    edge1_dir = 'IJ'
#                else:
#                    edge1_dir = 'JI'
#                edge2 = G.get_edge_data(path[y+1], path[y+2])
#                if edge2['INode'] == path[y+1]:
#                    edge2_dir = 'IJ'
#                else:
#                    edge2_dir = 'JI'
#                # if they are in the same direction, compare attributes
#                if edge2_dir == path_dir:
#                    compare_atts1 = {key: value for (key, value) in edge1.iteritems() if key in compare_cols}
#                    compare_atts2 = {key: value for (key, value) in edge2.iteritems() if key in compare_cols}
#                    if compare_atts1 == compare_atts2:
#                        merged_line = merge_edges(edge1['geometry'], edge2['geometry'], path_dir, edge1_dir, edge2_dir)
#                        edge1['geometry'] =  merged_line
#                        prev_merge = True
#                    else:
#                        edge_list.append(edge1)
#                        prev_merge = False
#                # they are in opposite directions, flip the ij attributes of the second edge
#                else:
#                    compare_atts1 = {key: value for (key, value) in edge1.iteritems() if key in compare_cols}
#                    # need to flip JI keys
#                    compare_atts2 = {key[1] + key[0] + key[2:] : value for (key, value) in edge2.iteritems() if key in ij_cols}
#                    compare_atts2.update({key: value for (key, value) in edge2.iteritems() if key in non_dir_cols})
#                    if compare_atts1 == compare_atts2:
#                        #flip line 2 and merge with line 1, add edge1 to edge list with merged lines
#                        merged_line = merge_edges(edge1['geometry'], edge2['geometry'], path_dir, edge1_dir, edge2_dir)
#                        edge1['geometry'] =  merged_line
#                        prev_merge = True
#                    else:
#                        edge_list.append(edge1)
#                        prev_merge = False

#            #Edge 1 exists as the result of the previous merge:
#            else:
#                edge2 = G.get_edge_data(path[y+1], path[y+2])
#                if edge2['INode'] == path[y+1]:
#                    edge2_dir = 'IJ'
#                else:
#                    edge2_dir = 'JI'
#                # if they are in the same direction, compare attributes
#                if edge2_dir == path_dir:
#                    compare_atts1 = {key: value for (key, value) in edge1.iteritems() if key in compare_cols}
#                    compare_atts2 = {key: value for (key, value) in edge2.iteritems() if key in compare_cols}
#                    if compare_atts1 == compare_atts2:
#                        #merge lines, add edge1 to edge list with merged lines
#                        merged_line = merge_edges(edge1['geometry'], edge2['geometry'], path_dir, edge1_dir, edge2_dir)
#                        edge1['geometry'] =  merged_line
#                        prev_merge = True
#                    else:
#                        if path_dir == 'IJ':
#                            edge1['JNode'] = path[y+1]
#                        else:
#                            edge1['INode'] = path[y+1]
#                        edge_list.append(edge1)
#                        prev_merge = False
#                # they are in opposite directions, flip the ij attributes of the second edge
#                else:
#                    compare_atts1 = {key: value for (key, value) in edge1.iteritems() if key in compare_cols}
#                    # need to flip JI keys
#                    compare_atts2 = {key[1] + key[0] + key[2:] : value for (key, value) in edge2.iteritems() if key in ij_cols}
#                    compare_atts2.update({key: value for (key, value) in edge2.iteritems() if key in non_dir_cols})
#                    if compare_atts1 == compare_atts2:
#                        #flip line 2 and merge with line 1, add edge1 to edge list with merged lines
#                        merged_line = merge_edges(edge1['geometry'], edge2['geometry'], path_dir, edge1_dir, edge2_dir)
#                        edge1['geometry'] =  merged_line
#                        prev_merge = True
#                    else:
#                        if path_dir == 'IJ':
#                            edge1['JNode'] = path[y+1]
#                        else:
#                            edge1['INode'] = path[y+1]
#                        edge_list.append(edge1)
#                        prev_merge = False
                               
#        ## IF the second node is the same as the second edge's INode, then it is going IJ
#        #if edge2['INode'] == path[y+1]:
#        #    # compare attributes in the IJ dir
#        #    compare_atts12 = {key: value for (key, value) in edge2.iteritems() if key in compare_cols}
#        #    if compare_atts1 == compare_atts2:
#        #        # merge lines
#        #        # update edge1 geometry, add to edge_list
#        ##        # 
#        ##if path[0] == edge_atts['INode']:
#        ##    edge_list.append({'PSRCEdgeID' : edge_atts['PSRCEdgeID'], 'dir' : 'IJ', 'order' : order, 'seg_id' : seg_id})
#        ##else:
#        ##    edge_list.append({'PSRCEdgeID' : edge_atts['PSRCEdgeID'], 'dir' : 'JI', 'order' : order, 'seg_id' : seg_id})
#        ##path.pop(0)
#        #order = order + 1
#    seg_id = seg_id+ 1

#print 'done!'

#crossroad_nodes = [node for node in G.nodes() if len(G.edges(node)) > 2]
#c_s = set(crossroad_nodes)
#for i in it.combinations(crossroad_nodes, 2):
#    if nx.has_path(G,source=i[0],target=i[1]):
#        path = nx.shortest_path(G,source=i[0],target=i[1])
#        #check to make sure path does not pass through another crossroad node
#        if len((c_s - set(i)) & set(path)) == 0:
#            print i,path



#for node in seg:
#    if f.node(node).degree == 1
##combine i j nodes
#test = pd.Series(df.INode.tolist() + df.JNode.tolist())
#df = scenario_edges[scenario_edges.INode.isin (seg) | scenario_edges.INode.isin(seg)]








#x = 0
#row_list = []
#for node in potential_thin_nodes:
#    #try:
#        df = gdf_TransRefEdges[(gdf_TransRefEdges.INode == node) | (gdf_TransRefEdges.JNode == node)]
#        assert len(df) == 2
#        a_row =  df.ix[df.index[0]]
#        #a = zip(a_row.geometry.xy[0], a_row.geometry.xy[1])
#        a_coords = list(a_row.geometry.coords)
#        a_test = [a_coords[0], a_coords[-1]]
#        b_row = df.ix[df.index[1]]
#        #b = zip(b_row.geometry.xy[0], b_row.geometry.xy[1])
#        b_coords = list(b_row.geometry.coords)
#        b_test = [b_coords[0], b_coords[-1]]

#        #if compare_attributes(a_row, b_row):
#            # Are lines digitized in the same direction?
#        if a_row.geometry.type == 'MultiLineString':
#            print 'Edge ' + str(a_row.PSRCEdgeID) + ' is a multi-part feature'
#        elif b_row.geometry.type == 'MultiLineString':
#            print 'Edge ' + str(b_row.PSRCEdgeID) + ' is a multi-part feature'
#        elif len(df.INode.value_counts())== 2 and len(df.JNode.value_counts()) == 2 and compare_attributes(a_row, b_row, 'IJ'):
                
#            if a_test.index(list(set(a_test).intersection(b_test))[0]) == 0 :
#                order = 'ba'
#                a_coords.pop(0)
#                x = b_coords + a_coords 
#                line = LineString(x)
#                merged_row = b_row
#                merged_row['geometry'] = line 
#                merged_row.JNode = int(a_row.JNode)
#                gdf_TransRefEdges = gdf_TransRefEdges[(gdf_TransRefEdges.PSRCEdgeID != int(a_row.PSRCEdgeID)) & (gdf_TransRefEdges.PSRCEdgeID != int(b_row.PSRCEdgeID))]
#                gdf_TransRefEdges.loc[gdf_TransRefEdges.index.max() + 1] = merged_row
#            else:
#                order = 'ab'
#                b_coords.pop(0)
#                x = a_coords + b_coords 
#                line = LineString(x)
#                merged_row = a_row
#                merged_row['geometry'] = line 
#                merged_row.JNode = int(b_row.JNode)
#                gdf_TransRefEdges = gdf_TransRefEdges[(gdf_TransRefEdges.PSRCEdgeID != int(a_row.PSRCEdgeID)) & (gdf_TransRefEdges.PSRCEdgeID != int(b_row.PSRCEdgeID))]
#                gdf_TransRefEdges.loc[gdf_TransRefEdges.index.max() + 1] = merged_row
        
#            # Are lines digitized towards each other:   
#        elif  len(df.JNode.value_counts()) == 1 and compare_attributes(a_row, b_row, 'JI'):
#            #Flip the b line
#            b_coords.reverse()
#            # drop the duplicate coord
#            b_coords.pop(0)
#            x = a_coords + b_coords
#            line = LineString(x)
#            merged_row = a_row
#            merged_row['geometry'] = line 
#            merged_row.INode = int(a_row.INode)
#            merged_row.JNode = int(b_row.INode)
#            gdf_TransRefEdges = gdf_TransRefEdges[(gdf_TransRefEdges.PSRCEdgeID != int(a_row.PSRCEdgeID)) & (gdf_TransRefEdges.PSRCEdgeID != int(b_row.PSRCEdgeID))]
#            gdf_TransRefEdges.loc[gdf_TransRefEdges.index.max() + 1] = merged_row

#        # Lines must be digitized away from each other:
#        else:
#             if compare_attributes(a_row, b_row, 'JI'):
#                # drop the duplicate coord
#                b_coords.pop(0)
#                #Flip the b line
#                b_coords.reverse()
#                x = b_coords + a_coords
#                line = LineString(x)
#                merged_row = a_row
#                merged_row['geometry'] = line 
#                merged_row.INode = int(b_row.JNode)
#                merged_row.JNode = int(a_row.JNode)
#                gdf_TransRefEdges = gdf_TransRefEdges[(gdf_TransRefEdges.PSRCEdgeID != int(a_row.PSRCEdgeID)) & (gdf_TransRefEdges.PSRCEdgeID != int(b_row.PSRCEdgeID))]
#                gdf_TransRefEdges.loc[gdf_TransRefEdges.index.max() + 1] = merged_row
        

    #except Exception, e:
    #    print e
    #    continue

scenario_edges.to_file(r'R:\Stefan\GDB_data\ScenarioEdges3.shp')

end_time = datetime.datetime.now()
elapsed_total = end_time - start_time
logger.info('------------------------RUN ENDING_----------------------------------------------')
logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))