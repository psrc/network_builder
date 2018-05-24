import time
import geopandas as gpd
import pandas as pd
import os
import numpy as np
import yaml
from log_controller import timed

config = yaml.safe_load(open("config.yaml"))

data_path = config['data_path']
model_year = config['model_year']


  
# modeAttributes
df_modeAttributes = pd.read_csv(os.path.join(data_path, 'modeAttributes.csv'))
# Edges
gdf_TransRefEdges = gpd.read_file(os.path.join(data_path, 'TransRefEdges.shp'))
gdf_TransRefEdges = gdf_TransRefEdges[gdf_TransRefEdges.length > 0]

#gdf_TransRefEdges = gpd.read_file(os.path.join(data_path, 'test.shp'))
gdf_TransRefEdges = gdf_TransRefEdges.merge(df_modeAttributes, how = 'left', on = 'PSRCEdgeID')

## TransitLines
gdf_TransitLines = gpd.read_file(os.path.join(data_path, 'TransitLines.shp'))
gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.InServiceD==model_year]
#gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.LineID == 114091]

### TransitPoints
gdf_TransitPoints = gpd.read_file(os.path.join(data_path, 'TransitPoints.shp'))
gdf_TransitPoints = gdf_TransitPoints[gdf_TransitPoints.LineID.isin(gdf_TransitLines.LineID)]



### Projects
gdf_ProjectRoutes = gpd.read_file(os.path.join(data_path, 'ProjectRoutes.shp'))

### tblLineProjects
df_tblLineProjects = pd.read_csv(os.path.join(data_path, 'tblLineProjects.csv'))
df_tblLineProjects = df_tblLineProjects[df_tblLineProjects.projRteID.isin(gdf_ProjectRoutes.projRteID)]

gdf_ProjectRoutes = gdf_ProjectRoutes.merge(df_tblLineProjects, how = 'left', on = 'projRteID')

##gdf_ProjectRoutes = gdf_ProjectRoutes[gdf_ProjectRoutes.projRteID.isin(project_list)]

## Turns
gdf_TurnMovements = gpd.read_file(os.path.join(data_path, 'TurnMovements.shp'))

## Juncions
gdf_Junctions = gpd.read_file(os.path.join(data_path, 'TransRefJunctions.shp'))
