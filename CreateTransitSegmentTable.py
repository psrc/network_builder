import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np
import networkx as nx
from networkx.algorithms.components import *
import numpy as np
import multiprocessing as mp
from multiprocessing import cpu_count, Pool
import pickle

class CreateTransitSegmentTable(object):
    def __init__(self, network_gdf, transit_routes_gdf, transit_points_gdf, time_period, config):
        self.network_gdf = network_gdf
        self.transit_routes_gdf = transit_routes_gdf
        self.transit_points_gdf = transit_points_gdf
        self.time_period = time_period
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
        self.cores = mp.cpu_count() #Number of CPU cores on your system
        self.partitions = 5
        self.transit_segments = self._get_transit_segments2() 
        
    def _test(self, row):
        points = self.transit_points_gdf[self.transit_points_gdf.LineID == row.LineID]
        points.sort_values('PointOrder', inplace = True)
        points_list = points['NewNodeID'].tolist()
        line_buff = row.geometry.buffer(100)
        line_buff = gpd.GeoDataFrame(pd.Series(line_buff), columns = ['geometry'])
        edges = gpd.sjoin(self.network_gdf, line_buff, how='inner',
                          op='within')
        G = nx.from_pandas_edgelist(edges, 'NewINode', 'NewJNode')
        
        seq_list = []
        while len(points_list) > 2:
            try:    
                seq_list.append(nx.shortest_path(G, points_list[0], points_list[1]))
                points_list.pop(0)
            except:
                #edges.to_file('d:/edges' + str(route_id) + '.shp')
                self._logger.info('No path between ' + str(points_list[0]) + 'and '+ str(points_list[1]) + ' in ' +  str(row.LineID))
                break
        return seq_list

    def _parallelize(self, df):
        a,b,c,d,e = np.array_split(df, self.partitions)
        pool = Pool(self.cores)
        df = pd.concat(pool.map(self._test, [a,b,c,d,e]))
        pool.close()
        pool.join()
        return df

    def test_func(self, df):
        print "Process working on: ",data
        #data["square"] = data["col"].apply(square)
        df['seq'] = df.apply(self._test);
        return data

    def _get_transit_segments(self):
        self.transit_points_gdf['NewNodeID'] = self.transit_points_gdf.PSRCJunctI + 4000
        #df = self.transit_routes_gdf.copy()
        self.transit_routes_gdf['seq'] = self.transit_routes_gdf.apply(self._test, axis=1)
        return self.transit_routes_gdf
        #test = self._parallelize(df)
        #print test

    def _get_transit_segments2(self):
        self.transit_points_gdf['NewNodeID'] = self.transit_points_gdf.PSRCJunctI + 4000
        #self.network_gdf['length'] = self.network_gdf.length
        #self.network_gdf['weight'] = self.network_gdf.length
        self.network_gdf['weight'] = np.where(self.network_gdf['FacilityTy'] == 99, .5 * self.network_gdf.length, self.network_gdf.length)

        route_id_list = self.transit_routes_gdf.LineID.tolist()
        i = 0
        row_list = []
        for route_id in route_id_list:
            if i % 10 == 0:
                print("%d Nodes Processed" % (i))
            line = self.transit_routes_gdf[self.transit_routes_gdf.LineID == route_id]
            points = self.transit_points_gdf[self.transit_points_gdf.LineID == route_id]
            points.sort_values('PointOrder', inplace = True)
            points_list = points['NewNodeID'].tolist()
            line['geometry'] = line.geometry.buffer(100)
            edges = gpd.sjoin(self.network_gdf, line, how='inner',
                          op='within')
            G = nx.from_pandas_edgelist(edges, 'NewINode', 'NewJNode', ['weight'])
        
            
            stop_number = 1
            order = 1
            while len(points_list) > 2:
                try:    
                    path = nx.shortest_path(G, points_list[0], points_list[1], 'weight')
                    while len(path) > 1:
                        row_list.append({'route_id' : route_id, 'INode' : path[0], 'JNode' : path[1], 'order' : order, 'stop_number' : stop_number})
                        order = order + 1
                        path.pop(0)
                    points_list.pop(0)
                    stop_number = stop_number + 1
                except:
                    edges.to_file('d:/edges' + str(route_id) + '.shp')

                    self._logger.info('No path between ' + str(points_list[0]) + 'and '+ str(points_list[1]) + ' in ' +  str(route_id))
                    break
            i = i + 1
        print 'done'
        pickle.dump(row_list, open("d:/row_list.p", "wb" ))
        return row_list



       

       




