import numpy as np
import pandas as pd
import inro.emme.desktop.app as app
import inro.modeller as _m
import inro.emme.matrix as ematrix
import inro.emme.database.matrix
import inro.emme.database.emmebank as _eb
import geopandas as gpd


bank = _eb.Emmebank(r'D:/stefan/test_2025/7to8/emmebank')
scenario = bank.scenario(1002)
network = scenario.get_network()

transit_lines = gpd.read_file('D:/stefan/GDB_Data/TransitLines.shp')
transit_lines = transit_lines[transit_lines.InServiceD==2014]
transit_lines = transit_lines.loc[transit_lines.Headway_AM > 0]

#transit_lines = pd.DataFrame(transit_lines)

transit_segs = pd.read_csv('d:/test_transit_segments.csv')
transit_segs.set_index('seg_id', inplace = True)
#transit_segs['is_stop'] = np.where(transit_segs.index.isin(transit_segs.stop_number.diff()[transit_segs.stop_number.diff() != 0].index.values), 1, 0)

for line in transit_lines.iterrows():
    line = line[1]
    segs = transit_segs.loc[transit_segs.route_id == line.LineID]
    if network.transit_line(line.LineID):
        network.delete_transit_line(line.LineID)
        print line.LineID
    if len(segs) == 1:
        emme_line = network.create_transit_line(line.LineID, line.VehicleTyp , [segs.INode, segs.JNode])
        #emme_line = network.create_transit_line(str(line.LineID)[-4:], line.VehicleTyp , [segs.INode, segs.JNode])
        
    else:
        nodes = segs.INode.tolist() + [segs.JNode.tolist()[-1]]
        emme_line = network.create_transit_line(line.LineID, line.VehicleTyp , nodes)
    emme_line.description = line.Descriptio
    emme_line.speed = line.Speed
    emme_line.speed = line.Headway_AM
    emme_line.data1 = line.Processing
    emme_line.data3 = line.Operator 

#transit_segs['loop_index'] = transit_segs.groupby(['route_id', 'INode', 'JNode']).cumcount()+1
#transit_segs['seg_id'] = transit_segs.route_id.astype(str) + '-' + transit_segs.INode.astype(str) + '-' + transit_segs.JNode.astype(str)
#transit_segs['seg_id'] = np.where(transit_segs.loop_index > 1, transit_segs.seg_id + '-' + transit_segs.loop_index.astype(str), transit_segs.seg_id) 


x = 0
for line in network.transit_lines():
    x = x + 1
    for seg in line.segments():
        row = transit_segs.ix[seg.id]
        seg.transit_time_func = row.ttf
        if line.mode == 'f' or line.mode == 'r' or line.mode == 'c':
            seg.allow_alightings = True
            seg.allow_boardings = True
            seg.dwell_time = 0
        if row.is_stop:
            seg.allow_alightings = True
            seg.allow_boardings = True
            seg.dwell_time = .25
        else:
            seg.allow_alightings = False
            seg.allow_boardings = False
            seg.dwell_time = False

        

    #try:
    #    emme_line = network.create_transit_line(line.LineID, line.VehicleTyp , segs.INode)
    #except:
    #    print 'line failure ' + str(line.LineID)
    #    pass

scenario.publish_network(network)


line_segs = transit_segs.loc[transit_segs.route_id == line.LineID]

#line_itinerary = line_segs.INode

emme_line = network.create_transit_line(line.LineID, line.VehicleTyp , line_segs.INode)


network.transit_line(transit_line.LineID)
network.transit_line(transit_ne.LineID)


