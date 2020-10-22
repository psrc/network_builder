import geopandas as gpd
import pandas as pd
import log_controller
import numpy as np

class ConfigureTransitSegments(object):
    def __init__(self, time_period, transit_segments, transit_lines, model_links, config):
        self.time_period = time_period
        self.transit_segments = transit_segments
        self.transit_lines = transit_lines
        self.model_links = model_links
        self.config = config
        self._logger = log_controller.logging.getLogger('main_logger')

    def configure(self):
        self.transit_segments.sort_values(['route_id', 'order'], inplace = True)
        self._report_errors()
        self._add_stop_column()
        self._add_stop_to_stop_distance_column()
        self._add_loop_index()
        self._add_segment_id()
        self._add_segment_ij_column()
        self._add_transit_mode_columns()
        self.transit_segments['ttf'] = self.transit_segments.apply(self._add_ttf, axis = 1)
        return self.transit_segments

    
    def _report_errors(self):
        if len(self.transit_segments[self.transit_segments['order'] == 9999]) > 0:
            for row in self.transit_segments[self.transit_segments['order'] == 9999].iterrows():
                self._logger.warning("Warning: No path between nodes %s and %s in %s route %s !" % (row[1].INode, row[1].JNode, self.time_period, row[1].route_id))
                self._logger.warning("Warning: Removing all segments from this route!")
                self.transit_segments = self.transit_segments[self.transit_segments['route_id'] != row[1].route_id]

        else:
             self._logger.warning("There are no errors in the %s Transit Segment Table" % (self.time_period))

    def _add_stop_column(self):
        self.transit_segments['is_stop'] = np.where(self.transit_segments.index.isin(self.transit_segments.stop_number.diff()[self.transit_segments.stop_number.diff() != 0].index.values), 1, 0)
        self.transit_segments['is_stop'] = np.where(self.transit_segments.index.isin(self.transit_segments.stop_number.diff()[self.transit_segments.stop_number.diff() != 0].index.values), 1, 0)

    def _add_stop_to_stop_distance_column(self):
        self.transit_segments = self.transit_segments.merge(self.model_links[['i', 'j', 'length']], how = 'left', left_on = ['INode', 'JNode'], right_on = ['i', 'j'])
        self.transit_segments['stop_to_stop_distance'] = self.transit_segments.groupby(['route_id', 'stop_number'])['length'].transform('sum')

    def _add_loop_index(self):
        self.transit_segments['loop_index'] = self.transit_segments.groupby(['route_id', 'INode', 'JNode']).cumcount() + 1

    def _add_segment_id(self):
        self.transit_segments['seg_id'] = self.transit_segments.route_id.astype(str) + '-' + self.transit_segments.INode.astype(str) + '-' + self.transit_segments.JNode.astype(str)
        self.transit_segments['seg_id'] = np.where(self.transit_segments.loop_index > 1, self.transit_segments.seg_id + '-' + self.transit_segments.loop_index.astype(str), self.transit_segments.seg_id)
         
    def _add_segment_ij_column(self):
        self.transit_segments['ij'] = self.transit_segments.INode.astype(str) + '-' + self.transit_segments.JNode.astype(str) 

    def _add_transit_mode_columns(self):
        self.transit_segments = self.transit_segments.merge(self.transit_lines[['Mode', 'LineID']], how = 'left', left_on = 'route_id', right_on = 'LineID') 
        self.transit_segments.rename(columns= {'Mode' : 'transit_mode'}, inplace = True)

        
    def _add_ttf(self, row):
        link = self.model_links.loc[row.ij]
        if row.transit_mode == "r" or row.transit_mode == "c" or row.transit_mode == "f":
            return 5
        elif link.mode == "bp" or link.mode == "bwlp" or link.mode == "brp" or link.mode == "bwp":
            return 4
        elif row.stop_to_stop_distance > 1.5:
            return 14
        elif (link.ul3 < 3).all() & (row.stop_to_stop_distance > .5):
            return 13
        elif row.stop_to_stop_distance > .5:
            return 12
        else:
            return 11
         
        
        coord_y = list(row['geometry_x'].coords)
        coord_x = list(row['geometry_y'].coords)
        return LineString(coord_x + coord_y) 


     






