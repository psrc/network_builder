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

#gdf_TransRefEdges = gdf_TransRefEdges[((gdf_TransRefEdges.InServiceD <= model_year) 
#                                      & (gdf_TransRefEdges.ActiveLink > 0) 
#                                      & (gdf_TransRefEdges.ActiveLink <> 999)) | gdf_TransRefEdges.PSRCEdgeID.isin(project_edges.edge_id.tolist())]


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
no_thin_edge_list = edges_from_turns(gdf_TurnMovements)
no_thin_node_list = nodes_from_edges(no_thin_edge_list, gdf_TransRefEdges)

# Get potential nodes to be thinned:
potential_thin_nodes = get_potential_thin_nodes(gdf_TransRefEdges)
potential_thin_nodes = [x for x in potential_thin_nodes if x not in no_thin_node_list]

logger.info(" %s Potential nodes to thin", len(potential_thin_nodes))

test = ThinNetwork(gdf_TransRefEdges, potential_thin_nodes)
scenario_edges = gdf_TransRefEdges


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

gdf_TransRefEdges.to_file(r'R:\Stefan\GDB_data\ScenarioEdges2.shp')

end_time = datetime.datetime.now()
elapsed_total = end_time - start_time
logger.info('------------------------RUN ENDING_----------------------------------------------')
logger.info('TOTAL RUN TIME %s'  % str(elapsed_total))