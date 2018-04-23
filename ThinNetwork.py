import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString
import log_controller

class ThinNetwork(object):
    def __init__(self, network_gdf, thin_nodes_list):
        self.network_gdf = network_gdf
        self.thin_nodes_list = thin_nodes_list
        self._logger = log_controller.logging.getLogger('main_logger')
        self.thinned_network_gdf = self._thin_network()
    
    def _compare_attributes(self, row1, row2, row2_dir= 'IJ'):
        df1 = pd.DataFrame(row1).T
        df2 = pd.DataFrame(row2).T
        # get all the directional columns
        ij_cols = [x for x in df1 if x[0:2] == "IJ" or x[0:2] == "JI" ]
        # switch IJ and JI columns
        ji_cols = [x[1] + x[0] + x[2:] for x in ij_cols]
        ij_cols = ['FacilityTy', 'Modes', 'Oneway', 'CountID', 'CountyID'] + ij_cols
        ji_cols = ['FacilityTy', 'Modes', 'Oneway', 'CountID', 'CountyID'] + ji_cols
        if row2_dir == 'IJ' and len(pd.merge(df1, df2, on=ij_cols)) > 0:
            return True
        elif row2_dir == 'JI' and len(pd.merge(df1, df2, right_on=ij_cols, left_on=ji_cols)) > 0:
            return True
        else:
            return False

    def _thin_network(self):
        i = 0
        for node in self.thin_nodes_list:
            if i % 1000 == 0:
                print("%d Nodes Processed" % (i))
            # get edges that have this node
            df = self.network_gdf[(self.network_gdf.INode == node) | (self.network_gdf.JNode == node)].copy()
            # only two edge intersections should be thinned
            if len(df) <> 2:
                print 'Node ' + str(node) + ' does not have exactly two edges'
            else:
                # get each edge
                a_row = df.ix[df.index[0]]
                b_row = df.ix[df.index[1]]
                if a_row.geometry.type =='MultiLineString':
                    print 'edge ' + str(int(a_row.PSRCEdgeID)) + ' is a multipart feature. Please fix.'  
                elif b_row.geometry.type =='MultiLineString':
                    print 'edge ' + str(int(b_row.PSRCEdgeID)) + ' is a multipart feature. Please fix.'  
                else:
                    a_coords = list(a_row.geometry.coords)
                    b_coords = list(b_row.geometry.coords)
                    # get the first and last coord for the two edges
                    a_test = [a_coords[0], a_coords[-1]]
                    b_test = [b_coords[0], b_coords[-1]]
                    # Are lines digitized in the same direction?
                    if len(df.INode.value_counts()) == 2 and len(df.JNode.value_counts()) == 2 and self._compare_attributes(a_row, b_row, 'IJ'):
                        # Do the first coords match or the first and last 
                        if len(list(set(a_test).intersection(b_test))) == 0:
                            print str(int(a_row.PSRCEdgeID)) + " " +  str(int(b_row.PSRCEdgeID)) + " are not connected!"
                        elif a_test.index(list(set(a_test).intersection(b_test))[0]) == 0 :
                            order = 'ba'
                            a_coords.pop(0)
                            x = b_coords + a_coords 
                            line = LineString(x)
                            merged_row = b_row
                            merged_row['geometry'] = line 
                            merged_row.JNode = int(a_row.JNode)
                            self.network_gdf = self.network_gdf[(self.network_gdf.PSRCEdgeID != int(a_row.PSRCEdgeID)) & (self.network_gdf.PSRCEdgeID != int(b_row.PSRCEdgeID))]
                            self.network_gdf.loc[self.network_gdf.index.max() + 1] = merged_row
                        else:
                            order = 'ab'
                            b_coords.pop(0)
                            x = a_coords + b_coords 
                            line = LineString(x)
                            merged_row = a_row
                            merged_row['geometry'] = line 
                            merged_row.JNode = int(b_row.JNode)
                            self.network_gdf = self.network_gdf[(self.network_gdf.PSRCEdgeID != int(a_row.PSRCEdgeID)) & (self.network_gdf.PSRCEdgeID != int(b_row.PSRCEdgeID))]
                            self.network_gdf.loc[self.network_gdf.index.max() + 1] = merged_row
        
                    # Are lines digitized towards each other:
                    elif  len(df.JNode.value_counts()) == 1 and self._compare_attributes(a_row, b_row, 'JI'):
                        #Flip the b line
                        b_coords.reverse()
                        # drop the duplicate coord
                        b_coords.pop(0)
                        x = a_coords + b_coords
                        line = LineString(x)
                        merged_row = a_row
                        merged_row['geometry'] = line 
                        merged_row.INode = int(a_row.INode)
                        merged_row.JNode = int(b_row.INode)
                        self.network_gdf = self.network_gdf[(self.network_gdf.PSRCEdgeID != int(a_row.PSRCEdgeID)) & (self.network_gdf.PSRCEdgeID != int(b_row.PSRCEdgeID))]
                        self.network_gdf.loc[self.network_gdf.index.max() + 1] = merged_row

                    # Lines must be digitized away from each other:
                    else:
                        if self._compare_attributes(a_row, b_row, 'JI'):
                            # drop the duplicate coord
                            b_coords.pop(0)
                            #Flip the b line
                            b_coords.reverse()
                            x = b_coords + a_coords
                            line = LineString(x)
                            merged_row = a_row
                            merged_row['geometry'] = line 
                            merged_row.INode = int(b_row.JNode)
                            merged_row.JNode = int(a_row.JNode)
                            self.network_gdf = self.network_gdf[(self.network_gdf.PSRCEdgeID != int(a_row.PSRCEdgeID)) & (self.network_gdf.PSRCEdgeID != int(b_row.PSRCEdgeID))]
                            self.network_gdf.loc[self.network_gdf.index.max() + 1] = merged_row
            i = i + 1
        return self.network_gdf