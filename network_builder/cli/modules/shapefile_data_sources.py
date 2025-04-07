import time
import geopandas as gpd
import pandas as pd
import os
import numpy as np
import yaml
from modules.log_controller import timed

config = yaml.safe_load(open("config.yaml"))

data_path = config["data_path"]
model_year = config["model_year"]


# modeAttributes
df_modeAttributes = pd.read_csv(os.path.join(data_path, "modeAttributes.csv"))

# Tolls
df_tolls = pd.read_csv(os.path.join(data_path, "modeTolls.csv"))
df_tolls = df_tolls[df_tolls["ModelYear"] == model_year]
df_tolls = df_tolls[config["toll_columns"] + config["dir_toll_columns"]]


# Edges
gdf_TransRefEdges = gpd.read_file(os.path.join(data_path, "TransRefEdges.shp"))
gdf_TransRefEdges = gdf_TransRefEdges[gdf_TransRefEdges.length > 0]


# gdf_TransRefEdges = gpd.read_file(os.path.join(data_path, 'test.shp'))
gdf_TransRefEdges = gdf_TransRefEdges.merge(
    df_modeAttributes, how="left", on="PSRCEdgeID"
)
gdf_TransRefEdges.crs = config["crs"]

gdf_TransRefEdges = gdf_TransRefEdges.merge(df_tolls, how="left", on="PSRCEdgeID")
gdf_TransRefEdges.fillna(0, inplace=True)


## TransitLines
gdf_TransitLines = gpd.read_file(os.path.join(data_path, "TransitLines.shp"))
gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.InServiceD == model_year]
gdf_TransitLines.crs = config["crs"]
# gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.LineID == 114091]

### TransitPoints
gdf_TransitPoints = gpd.read_file(os.path.join(data_path, "TransitPoints.shp"))
gdf_TransitPoints = gdf_TransitPoints[
    gdf_TransitPoints.LineID.isin(gdf_TransitLines.LineID)
]
gdf_TransitPoints.crs = config["crs"]

### Projects
if config["update_network_from_projects"]:
    gdf_ProjectRoutes = gpd.read_file(os.path.join(data_path, "ProjectRoutes.shp"))
    gdf_ProjectRoutes["FacilityType"] = gdf_ProjectRoutes["Change_Typ"].astype(int)
    gdf_ProjectRoutes.crs = config["crs"]
else:
    gdf_ProjectRoutes = None


### tblLineProjects
if config["update_network_from_projects"]:
    df_tblLineProjects = pd.read_csv(os.path.join(data_path, "tblLineProjects.csv"))
    df_tblLineProjects = df_tblLineProjects[
        df_tblLineProjects.projRteID.isin(gdf_ProjectRoutes.projRteID)
    ]

# Point Events (Projects that change capacity of a Park and Ride)
if config["update_network_from_projects"]:
    df_evtPointProjectOutcomes = pd.read_csv(
        os.path.join(data_path, "evtPointProjectOutcomes.csv")
    )
else:
    df_evtPointProjectOutcomes = None

if config["update_network_from_projects"]:
    gdf_ProjectRoutes = gdf_ProjectRoutes.merge(
        df_tblLineProjects, how="left", on="projRteID"
    )
    gdf_ProjectRoutes = gdf_ProjectRoutes.loc[
        gdf_ProjectRoutes["InServiceDate"] <= config["model_year"]
    ]

##gdf_ProjectRoutes = gdf_ProjectRoutes[gdf_ProjectRoutes.projRteID.isin(project_list)]

## Turns
gdf_TurnMovements = gpd.read_file(os.path.join(data_path, "TurnMovements.shp"))
gdf_TurnMovements = gdf_TurnMovements[
    gdf_TurnMovements["InServiceD"] <= config["model_year"]
]
gdf_TurnMovements.crs = config["crs"]

## Juncions

gdf_Junctions = gpd.read_file(os.path.join(data_path, "TransRefJunctions.shp"))
gdf_Junctions.crs = config["crs"]


# Transit Frequencies:
if config["build_transit_headways"]:
    df_transit_frequencies = pd.read_csv(
        os.path.join(data_path, "transitFrequency.csv")
    )
