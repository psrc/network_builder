import os
import pyodbc
import sqlalchemy
from pymssql import connect
import time
from pandas import read_sql
from shapely import wkt
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
import pandas as pd
import os
import numpy as np
import yaml

import time
import sys


def read_from_sde(
    config,
    feature_class_name,
    is_table=False,
):
    """
    Returns the specified feature class as a geodataframe from ElmerGeo.

    Parameters
    ----------
    connection_string : SQL connection string that is read by geopandas
                        read_sql function

    feature_class_name: the name of the featureclass in PSRC's ElmerGeo
                        Geodatabase

    cs: cordinate system
    """
    if config.use_sqlalchemy:
        connection_string = (
            """mssql+pyodbc://%s/%s?driver=SQL Server?Trusted_Connection=yes"""
            % (config.server, config.database)
        )
        # connection_string = '''mssql+pyodbc://%s/%s?driver=ODBC Driver 17 for SQL Server?Trusted_Connection=yes''' % (config['server'], config['database'])
        engine = sqlalchemy.create_engine(connection_string)
        con = engine.connect()
        # con.execute("dbo.set_current_version {0}".format(version))
        con.execute(f"{config.sde_schema}.set_current_version {config.version}")

    else:
        con = connect(config.server, database=config.database)
        cursor = con.cursor()
        # cursor.execute("dbo.set_current_version %s", version[1:-1])
        cursor.execute(f"{config.sde_schema}.set_current_version {config.version}")

    if is_table:
        gdf = pd.read_sql("select * from %s" % (feature_class_name), con=con)
        con.close()

    else:
        if config.use_sqlalchemy:
            query_string = "select *, Shape.STAsText() as geometry from %s" % (
                feature_class_name
            )
        else:
            geo_col_stmt = (
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME="
                + "'"
                + feature_class_name
                + "'"
                + " AND DATA_TYPE='geometry'"
            )
            try:
                geo_col = str(pd.read_sql(geo_col_stmt, con).iloc[0, 0])
            except:
                geo_col = "Shape"
            query_string = (
                "SELECT *,"
                + geo_col
                + ".STGeometryN(1).ToString()"
                + " FROM "
                + feature_class_name
            )
        df = pd.read_sql(query_string, con)
        con.close()
        df.rename(columns={"": "geometry"}, inplace=True)

        df["geometry"] = df["geometry"].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        gdf.crs = config.input_crs
        cols = [
            col
            for col in gdf.columns
            if col not in ["Shape", "GDB_GEOMATTR_DATA", "SDE_STATE_ID"]
        ]
        gdf = gdf[cols]
        if (not is_table) and (config.output_crs):
            gdf = gdf.to_crs(config.output_crs)

    return gdf


def open_from_file_gdb(config, layer_name, is_table=False):
    
    gdf = gpd.read_file(config.file_gdb_path, layer=layer_name)
    if not is_table:
        gdf = gdf.explode()
    if config.output_crs:
        gdf = gdf.to_crs(config.output_crs)
    return gdf


class NetworkData:
    def __init__(self, config, tables_config):
        self.config = config
        self.tables_config = tables_config
        # data:
        self.df_tolls = self.get_tolls()
        self.gdf_TransitLines = self.get_transit_lines()
        self.gdf_TransitPoints = self.get_transit_points()
        self.gdf_TurnMovements = self.get_turns()
        self.gdf_Junctions = self.get_data(self.tables_config.junctions, False)
        self.gdf_TransRefEdges = self.get_edges()
        self.df_transit_frequencies = self.get_transit_frequencies()
        # projects:
        if config.update_network_from_projects:
            self.gdf_ProjectRoutes = self.get_projects()
            self.df_evtPointProjectOutcomes = self.get_data(
                self.tables_config.point_events, True
            )
        else:
            self.gdf_ProjectRoutes = None
            self.df_evtPointProjectOutcomes = None

    def get_data(self, layer_name, is_table):
        if self.config.data_source_type == "enterprise_gdb":
            return read_from_sde(self.config, layer_name, is_table=is_table)
        else:
            return open_from_file_gdb(self.config, layer_name, is_table)

    def get_tolls(self):
        df = self.get_data(self.tables_config.mode_tolls, True)
        df = df[df["InServiceDate"] == self.config.model_year]
        return df[self.config.toll_columns + self.config.dir_toll_columns]

    def get_transit_lines(self):
        gdf = self.get_data(self.tables_config.transit_lines, False)
        return gdf[gdf["InServiceDate"] == self.config.model_year]

    def get_transit_points(self):
        gdf = self.get_data(self.tables_config.transit_points, False)
        return gdf[gdf.LineID.isin(self.gdf_TransitLines.LineID)]

    def get_transit_frequencies(self):
        df = self.get_data(self.tables_config.transit_frequencies, True)
        return df[df.LineID.isin(self.gdf_TransitLines.LineID)]

    def get_turns(self):
        gdf = self.get_data(self.tables_config.turn_movements, False)
        return gdf[gdf["InServiceDate"] <= self.config.model_year]

    def get_edges(self):
        gdf = self.get_data(self.tables_config.edges, False)
        if self.tables_config.mode_attributes:
            df_mode_attributes = self.get_data(
                self.tables_config.mode_attributes, is_table=True
            )
            gdf = gdf.merge(df_mode_attributes, how="left", on="PSRCEdgeID")
        gdf = gdf.merge(self.df_tolls, how="left", on="PSRCEdgeID")
        fill_colls = [col for col in gdf.columns if "geom" not in col]
        for col in fill_colls:
            gdf[col].fillna(0, inplace=True)
        return gdf

    def get_projects(self):
        gdf_ProjectRoutes = self.get_data(self.tables_config.project_routes, False)
        gdf_ProjectRoutes = gdf_ProjectRoutes[
            gdf_ProjectRoutes["version"] == self.config.projects_version_year
        ]
        gdf_ProjectRoutes["FacilityType"] = gdf_ProjectRoutes["Change_Type"].astype(int)

        # scenario:
        df_tblProjectsInScenarios = self.get_data(
            self.tables_config.projects_in_scenarios, True
        )
        df_tblProjectsInScenarios = df_tblProjectsInScenarios[
            df_tblProjectsInScenarios["ScenarioName"] == self.config.scenario_name
        ]
        gdf_ProjectRoutes = gdf_ProjectRoutes[
            gdf_ProjectRoutes["intProjID"].isin(df_tblProjectsInScenarios["intProjID"])
        ]
        df_tblLineProjects = self.get_data(self.tables_config.project_attributes, True)
        # project attributes
        df_tblLineProjects = df_tblLineProjects[
            df_tblLineProjects.projRteID.isin(gdf_ProjectRoutes.projRteID)
        ]

        gdf_ProjectRoutes = gdf_ProjectRoutes.merge(
            df_tblLineProjects, how="left", on="projRteID"
        )
        return gdf_ProjectRoutes.loc[
            gdf_ProjectRoutes["InServiceDate"] <= self.config.model_year
        ]


# config = yaml.safe_load(
#     open(os.path.join(modules.configuration.args.configs_dir, "config.yaml"))
# )

# config = ValidateSettings(**config)

# tables_config = yaml.safe_load(
#     open(os.path.join(modules.configuration.args.configs_dir, "tables_config.yaml"))
# )

# config = config.dict()
# model_year = config["model_year"]
# input_crs = config["input_crs"]
# output_crs = config["output_crs"]


# if config["data_source_type"] == "enterprise_gdb":


#     version = config["version"]

#     # modeAttributes
#     if "mode_attributes" in tables_config.keys():
#         df_modeAttributes = read_from_sde(
#             config,
#             tables_config["mode_attributes"],
#             version,
#             crs=input_crs,
#             output_crs=output_crs,
#             is_table=True,
#         )

#     df_tolls = read_from_sde(
#         config,
#         tables_config["mode_tolls"],
#         version,
#         crs=input_crs,
#         output_crs=output_crs,
#         is_table=True,
#     )

#     gdf_TransRefEdges = read_from_sde(
#         config,
#         tables_config["edges"],
#         version,
#         crs=input_crs,
#         output_crs=output_crs,
#         is_table=False,
#     )

#     gdf_TransitLines = read_from_sde(
#         config,
#         tables_config["transit_lines"],
#         version,
#         crs=input_crs,
#         output_crs=output_crs,
#         is_table=False,
#     )

#     gdf_TransitPoints = read_from_sde(
#         config,
#         tables_config["transit_points"],
#         version,
#         crs=input_crs,
#         output_crs=output_crs,
#         is_table=False,
#     )
#     gdf_TurnMovements = read_from_sde(
#         config,
#         tables_config["turn_movements"],
#         version,
#         crs=input_crs,
#         output_crs=output_crs,
#         is_table=False,
#     )

#     # Juncions
#     gdf_Junctions = read_from_sde(
#         config,
#         tables_config["junctions"],
#         version,
#         crs=input_crs,
#         output_crs=output_crs,
#         is_table=False,
#     )

#     if config["build_transit_headways"]:
#         df_transit_frequencies = read_from_sde(
#             config,
#             tables_config["transit_frequencies"],
#             version,
#             crs=input_crs,
#             is_table=True,
#         )

#     if config["update_network_from_projects"]:
#         gdf_ProjectRoutes = read_from_sde(
#             config,
#             tables_config["project_routes"],
#             version,
#             crs=input_crs,
#             output_crs=output_crs,
#             is_table=False,
#         )

#         df_tblProjectsInScenarios = read_from_sde(
#             config,
#             tables_config["projects_in_scenarios"],
#             version,
#             crs=input_crs,
#             output_crs=output_crs,
#             is_table=True,
#         )

#         df_tblLineProjects = read_from_sde(
#             config,
#             tables_config["project_attributes"],
#             version,
#             crs=input_crs,
#             is_table=True,
#         )

#         # point events (park and rides)
#         df_evtPointProjectOutcomes = read_from_sde(
#             config, tables_config["point_events"], version, crs=input_crs, is_table=True
#         )

#     else:
#         gdf_ProjectRoutes = None
#         df_tblProjectsInScenarios = None
#         df_evtPointProjectOutcomes = None

# else:
#     if "mode_attributes" in tables_config.keys():
#         df_modeAttributes = open_from_file_gdb(
#             config["file_gdb_path"], tables_config["mode_attributes"]
#         )

#     #df_modeAttributes.drop(columns=["geometry"], inplace=True)

#     df_tolls = open_from_file_gdb(config["file_gdb_path"], tables_config["mode_tolls"])
#     #df_tolls = df_tolls.drop("geometry", 1)

#     gdf_TransRefEdges = open_from_file_gdb(
#         config["file_gdb_path"], tables_config["edges"], output_crs=output_crs
#     )

#     gdf_TransRefEdges = gdf_TransRefEdges.explode()
#     #gdf_TransRefEdges.index = gdf_TransRefEdges.index.droplevel(1)

#     gdf_TransitLines = open_from_file_gdb(
#         config["file_gdb_path"], tables_config["transit_lines"], output_crs=output_crs
#     )

#     gdf_TransitLines = gdf_TransitLines.explode()
#     #gdf_TransitLines.index = gdf_TransitLines.index.droplevel(1)

#     gdf_TransitPoints = open_from_file_gdb(
#         config["file_gdb_path"], tables_config["transit_points"], output_crs=output_crs
#     )

#     gdf_TurnMovements = open_from_file_gdb(
#         config["file_gdb_path"], tables_config["turn_movements"], output_crs=output_crs
#     )

#     gdf_TurnMovements = gdf_TurnMovements.explode()
#     #gdf_TurnMovements.index = gdf_TurnMovements.index.droplevel(1)

#     # Juncions
#     gdf_Junctions = open_from_file_gdb(
#         config["file_gdb_path"], tables_config["junctions"], output_crs=output_crs
#     )

#     if config["build_transit_headways"]:
#         df_transit_frequencies = gpd.read_file(
#             config["file_gdb_path"], layer=tables_config["transit_frequencies"]
#         )

#         #df_transit_frequencies.drop(columns=["geometry"], inplace = True)

#     if config["update_network_from_projects"]:
#         gdf_ProjectRoutes = open_from_file_gdb(
#             config["file_gdb_path"],
#             tables_config["project_routes"],
#             output_crs=output_crs,
#         )

#         gdf_ProjectRoutes = gdf_ProjectRoutes.explode()
#         #gdf_ProjectRoutes.index = gdf_ProjectRoutes.index.droplevel(1)

#         df_tblProjectsInScenarios = gpd.read_file(
#             config["file_gdb_path"], layer=tables_config["projects_in_scenarios"]
#         )

#         #df_tblProjectsInScenarios.drop(columns=["geometry"], inplace = True)

#         df_tblLineProjects = gpd.read_file(
#             config["file_gdb_path"], layer=tables_config["project_attributes"]
#         )

#         #df_tblLineProjects.drop(columns=["geometry"], inplace = True)
#         # point events (park and rides)
#         df_evtPointProjectOutcomes = gpd.read_file(
#             config["file_gdb_path"], layer=tables_config["point_events"]
#         )

#         #df_evtPointProjectOutcomes.drop(columns=["geometry"], inplace=True)
#     else:
#         gdf_ProjectRoutes = None
#         df_tblProjectsInScenarios = None
#         df_evtPointProjectOutcomes = None


# # Tolls
# df_tolls = df_tolls[df_tolls["InServiceDate"] == model_year]
# df_tolls = df_tolls[config["toll_columns"] + config["dir_toll_columns"]]

# # Edges
# gdf_TransRefEdges = gdf_TransRefEdges[gdf_TransRefEdges.length > 0]

# if "mode_attributes" in tables_config.keys():
#     gdf_TransRefEdges = gdf_TransRefEdges.merge(
#         df_modeAttributes, how="left", on="PSRCEdgeID"
#     )

# gdf_TransRefEdges = gdf_TransRefEdges.merge(df_tolls, how="left", on="PSRCEdgeID")

# fill_colls = [col for col in gdf_TransRefEdges.columns if "geom" not in col]
# for col in fill_colls:
#     gdf_TransRefEdges[col].fillna(0, inplace=True)

# # TransitLines
# gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.InServiceDate == model_year]

# # TransitPoints
# gdf_TransitPoints = gdf_TransitPoints[
#     gdf_TransitPoints.LineID.isin(gdf_TransitLines.LineID)
# ]

# # Projects
# if config["update_network_from_projects"]:
#     gdf_ProjectRoutes = gdf_ProjectRoutes[
#         gdf_ProjectRoutes["version"] == config["projects_version_year"]
#     ]

#     gdf_ProjectRoutes["FacilityType"] = gdf_ProjectRoutes["Change_Type"].astype(int)

#     # scenarios:
#     df_tblProjectsInScenarios = df_tblProjectsInScenarios[
#         df_tblProjectsInScenarios["ScenarioName"] == config["scenario_name"]
#     ]

#     gdf_ProjectRoutes = gdf_ProjectRoutes[
#         gdf_ProjectRoutes["intProjID"].isin(df_tblProjectsInScenarios["intProjID"])
#     ]

#     # project attributes
#     df_tblLineProjects = df_tblLineProjects[
#         df_tblLineProjects.projRteID.isin(gdf_ProjectRoutes.projRteID)
#     ]

#     gdf_ProjectRoutes = gdf_ProjectRoutes.merge(
#         df_tblLineProjects, how="left", on="projRteID"
#     )
#     gdf_ProjectRoutes = gdf_ProjectRoutes.loc[
#         gdf_ProjectRoutes["InServiceDate"] <= config["model_year"]
#     ]


# # Turns
# gdf_TurnMovements = gdf_TurnMovements[
#     gdf_TurnMovements["InServiceDate"] <= config["model_year"]
# ]
