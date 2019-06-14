import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np
from shapely.geometry import LineString
from shapely.geometry import Point

class TransitHeadways(object):

    def __init__(self, transit_gdf, df_transit_frequenciies, config):
        self.transit_routes = transit_gdf
        self.frequencies = df_transit_frequenciies
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
    
    def build_headways(self):
        frequency_df = self.transit_routes.merge(self.frequencies, how = 'left', left_on = 'LineID', right_on = 'LineID')
        frequency_df['id'] = frequency_df['LineID']
        col_list = ['LineID', 'id']

        for k, v in self.config['transit_headway_mapper'].iteritems():
            frequency_df[k] = frequency_df[v].sum(axis=1)
            frequency_df[k] = (60*len(v))/frequency_df[k]
            frequency_df[k] = frequency_df[k].replace(np.inf, 0)
            col_list.append(k) 
        frequency_df = frequency_df[col_list]
        #frequency_df = frequency_df.rename(columns = {'Processing' : 'id'})
        return frequency_df

