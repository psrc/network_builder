import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np
from shapely.geometry import LineString
from shapely.geometry import Point

class BuildZoneInputs(object):

    def __init__(self, scenario_junctions, projects_gdf, point_events_df, config ):
        self.scenario_junctions = scenario_junctions
        self.project_gdf = projects_gdf
        self.point_events_df = point_events_df
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')
    
    def build_zone_inputs(self):
        zone_nodes_df = self.scenario_junctions.loc[self.scenario_junctions['is_zone'] == 1]
        zone_nodes_df['i'] = zone_nodes_df['i'].astype(int)

        zone_nodes_df['XCoord'] = zone_nodes_df.geometry.x
        zone_nodes_df['YCoord'] = zone_nodes_df.geometry.y
        if self.config['update_network_from_projects']:
            zone_nodes_df.set_index('PSRCjunctID', inplace = True)
            p_r_projects = self.project_gdf[self.project_gdf['withEvents'] == 2]['projRteID'].tolist()
            p_r_projects_df = self.point_events_df.loc[self.point_events_df['projRteID'].isin(p_r_projects)].copy()
            p_r_projects_df.set_index('PSRCJunctID', inplace =  True)
            zone_nodes_df.update(p_r_projects_df)
            zone_nodes_df.reset_index(inplace = True)
        
        # Scen_Node is the taz id column
        zone_nodes_df = zone_nodes_df.sort_values(by='i')

        # create an ordinal/index column. Daysim is 1 based. 
        zone_nodes_df['zone_ordinal'] = [i for i in range(1, len(zone_nodes_df) + 1)]

        # create a cost columnn for park and rides, set to 0 for now
        zone_nodes_df['Cost'] = 0

        # column for non park & rides, internal zones
        zone_nodes_df['Dest_eligible'] = 0
        zone_nodes_df.loc[zone_nodes_df['i'] <= self.config['max_regular_zone'], 'Dest_eligible'] = 1

        # rename some columns for the taz file
        zone_nodes_df = zone_nodes_df.rename(columns={'i': 'Zone_id', 'P_RStalls': 'Capacity', 'Processing' : 'External'})
        
        p_r_df = zone_nodes_df.loc[zone_nodes_df['Capacity'] > 0].copy()
        # rename some columns for the park and ride file

        p_r_df = p_r_df.rename(columns={'Zone_id': 'ZoneID', 'zone_ordinal' : 'NodeID', })
        
        return (zone_nodes_df, p_r_df)
