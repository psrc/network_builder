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
gdf_TransRefEdges.crs = {'init' : 'EPSG:2285'}


## TransitLines
gdf_TransitLines = gpd.read_file(os.path.join(data_path, 'TransitLines.shp'))
gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.InServiceD==model_year]
gdf_TransitLines.crs = {'init' : 'EPSG:285'}
#gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.LineID == 114091]

### TransitPoints
gdf_TransitPoints = gpd.read_file(os.path.join(data_path, 'TransitPoints.shp'))
gdf_TransitPoints = gdf_TransitPoints[gdf_TransitPoints.LineID.isin(gdf_TransitLines.LineID)]
gdf_TransitPoints.crs = {'init' : 'EPSG:2285'}

### Projects
if config['update_network_from_projects']:
    gdf_ProjectRoutes = gpd.read_file(os.path.join(data_path, 'ProjectRoutes.shp'))
    gdf_ProjectRoutes['FacilityTy'] = gdf_ProjectRoutes['Change_Typ']
    gdf_ProjectRoutes.crs = {'init' : 'EPSG:2285'}
else:
    gdf_ProjectRoutes = None


### tblLineProjects
if config['update_network_from_projects']:
    #df_tblLineProjects = pd.read_csv(os.path.join(data_path, 'tblLineProjects.csv'))
    df_tblLineProjects = df_tblLineProjects[df_tblLineProjects.projRteID.isin(gdf_ProjectRoutes.projRteID)]

# Point Events (Projects that change capacity of a Park and Ride)
if config['update_network_from_projects']:
    df_evtPointProjectOutcomes = pd.read_csv(os.path.join(data_path, 'evtPointProjectOutcomes.csv'))
else:
    df_evtPointProjectOutcomes = None

if config['update_network_from_projects']:
    gdf_ProjectRoutes = gdf_ProjectRoutes.merge(df_tblLineProjects, how = 'left', on = 'projRteID')
    gdf_ProjectRoutes = gdf_ProjectRoutes.loc[gdf_ProjectRoutes['InServiceDate'] <= config['model_year']]

##gdf_ProjectRoutes = gdf_ProjectRoutes[gdf_ProjectRoutes.projRteID.isin(project_list)]

## Turns
gdf_TurnMovements = gpd.read_file(os.path.join(data_path, 'TurnMovements.shp'))
gdf_TurnMovements = gdf_TurnMovements[gdf_TurnMovements['InServiceD'] <= config['model_year']]
gdf_TurnMovements.crs = {'init' : 'EPSG:2285'}

## Juncions

gdf_Junctions = gpd.read_file(os.path.join(data_path, 'TransRefJunctions.shp'))
gdf_Junctions.crs = {'init' : 'EPSG:2285'}