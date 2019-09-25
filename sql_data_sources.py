from pymssql import connect
from pandas import read_sql
from shapely.wkt import loads
import geopandas as gpd
from geopandas import GeoDataFrame
import pandas as pd
import os
import numpy as np
import yaml
from log_controller import timed
import time
import sys

def rd_sql(server, database, table, version,  col_names=None, where_col=None, where_val=None, geo_col=False, epsg=2193, export=False, path='save.csv'):
    """
    Imports data from MSSQL database, returns GeoDataFrame. Specific columns can be selected and specific queries within columns can be selected. Requires the pymssql package, which must be separately installed.
    Arguments:
    server -- The server name (str). e.g.: 'SQL2012PROD03'
    database -- The specific database within the server (str). e.g.: 'LowFlows'
    table -- The specific table within the database (str). e.g.: 'LowFlowSiteRestrictionDaily'
    col_names -- The column names that should be retrieved (list). e.g.: ['SiteID', 'BandNo', 'RecordNo']
    where_col -- The sql statement related to a specific column for selection (must be formated according to the example). e.g.: 'SnapshotType'
    where_val -- The WHERE query values for the where_col (list). e.g. ['value1', 'value2']
    geo_col -- Is there a geometry column in the table?
    epsg -- The coordinate system (int)
    export -- Should the data be exported
    path -- The path and csv name for the export if 'export' is True (str)
    """
    if col_names is None and where_col is None:
        stmt1 = 'SELECT * FROM ' + table
    elif where_col is None:
        stmt1 = 'SELECT ' + str(col_names).replace('\'', '"')[1:-1] + ' FROM ' + table
    else:
        stmt1 = 'SELECT * FROM ' + table + ' WHERE ' + str([where_col]).replace('\'', '"')[1:-1] + ' IN (' + str(where_val)[1:-1] + ')'
    conn = connect(server, database=database)
    cursor = conn.cursor()
    cursor.execute("sde.set_current_version %s", version)
    df = read_sql(stmt1, conn)

    ## Read in geometry if required
    if geo_col:
        geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + table + "\'" + " AND DATA_TYPE='geometry'"
        geo_col = str(read_sql(geo_col_stmt, conn).iloc[0,0])
        if where_col is None:
            stmt2 = 'SELECT ' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + table
        else:
            stmt2 = 'SELECT ' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + table + ' WHERE ' + str([where_col]).replace('\'', '"')[1:-1] + ' IN (' + str(where_val)[1:-1] + ')'
        df2 = read_sql(stmt2, conn)
        df2.columns = ['geometry']
        try:

        #test = np.array_split(df2, 3)
            geometry = map(lambda x: loads(x), df2.geometry) 
        #geometry2 = map(lambda x: loads(x), test[2].geometry)
        #geometry = map(loads, df2.geometry)
        #geometry = [loads(x) for x in df2.geometry]
        except:
            print ('Loading Geometry for %s failed. Check to make there are no 0 length features or feature that contain M values. Exiting Program!' % table)

        df = GeoDataFrame(df, geometry=geometry, crs={'init' :'epsg:' + str(epsg)})
        if 'Shape' in df.columns:
            df.drop(['Shape'], axis = 1, inplace = True)

    if export:
        df.to_csv(path, index=False)

    conn.close()
    return(df)

config = yaml.safe_load(open("config.yaml"))

data_path = config['data_path']
model_year = config['model_year']
server = config['server']
database = config['database']
version = config['version']
epsg = config['epsg']

# modeAttributes
df_modeAttributes = rd_sql(server, database, 'modeAttributes_evw', version, None, None, None, False, epsg=epsg)

# Tolls
df_tolls = rd_sql(server, database, 'modeTolls_evw', version, None, None, None, False, epsg=epsg)
df_tolls = df_tolls[df_tolls['InServiceDate'] == model_year]
df_tolls = df_tolls[config['toll_columns'] + config['dir_toll_columns']]

# Edges
gdf_TransRefEdges = rd_sql(server, database, 'TransRefEdges_evw', version, None, None, None, True, epsg=epsg)
gdf_TransRefEdges = gdf_TransRefEdges[gdf_TransRefEdges.length > 0]


#gdf_TransRefEdges = gpd.read_file(os.path.join(data_path, 'test.shp'))
gdf_TransRefEdges = gdf_TransRefEdges.merge(df_modeAttributes, how = 'left', on = 'PSRCEdgeID')

gdf_TransRefEdges = gdf_TransRefEdges.merge(df_tolls, how = 'left', on = 'PSRCEdgeID')
gdf_TransRefEdges.fillna(0, inplace = True)


## TransitLines
gdf_TransitLines = rd_sql(server, database, 'TransitLines_evw', version, None, None, None, True, epsg=epsg)
gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.InServiceDate==model_year]

### TransitPoints
gdf_TransitPoints = rd_sql(server, database, 'TransitPoints_evw', version, None, None, None, True, epsg=epsg)
gdf_TransitPoints = gdf_TransitPoints[gdf_TransitPoints.LineID.isin(gdf_TransitLines.LineID)]

### Projects
if config['update_network_from_projects']:
    #gdf_ProjectRoutes = gpd.read_file(os.path.join(data_path, 'ProjectRoutes.shp'))
    gdf_ProjectRoutes = rd_sql(server, database, 'ProjectRoutes_evw', version, None, 'version', [2018], True, epsg=epsg)
    gdf_ProjectRoutes['FacilityType'] = gdf_ProjectRoutes['Change_Type'].astype(int)
    
    #scenarios:
    df_tblProjectsInScenarios = rd_sql(server, database, 'tblProjectsInScenarios_evw', version, None, 'ScenarioName', [config['scenario_name']], False, epsg=epsg)
    gdf_ProjectRoutes = gdf_ProjectRoutes[gdf_ProjectRoutes['intProjID'].isin(df_tblProjectsInScenarios['intProjID'])]

    # project attributes
    df_tblLineProjects = rd_sql(server, database, 'tblLineProjects_evw', version, None, None, None, False, epsg=epsg)
    df_tblLineProjects = df_tblLineProjects[df_tblLineProjects.projRteID.isin(gdf_ProjectRoutes.projRteID)]

    # point events (park and rides)
    df_evtPointProjectOutcomes = rd_sql(server, database, 'evtPointProjectOutcomes', version, None, None, None, False, epsg=epsg)

    gdf_ProjectRoutes = gdf_ProjectRoutes.merge(df_tblLineProjects, how = 'left', on = 'projRteID')
    gdf_ProjectRoutes = gdf_ProjectRoutes.loc[gdf_ProjectRoutes['InServiceDate'] <= config['model_year']]
    # drop InServiceDate as it is on edges
    #gdf_ProjectRoutes.drop(['InServiceDate'], axis = 1, inplace = True)

else:
    gdf_ProjectRoutes = None
    df_tblProjectsInScenarios = None
    df_evtPointProjectOutcomes = None


## Turns
gdf_TurnMovements = rd_sql(server, database, 'TurnMovements_evw', version, None, None, None, True, epsg=epsg)
gdf_TurnMovements = gdf_TurnMovements[gdf_TurnMovements['InServiceDate'] <= config['model_year']]

## Juncions
gdf_Junctions = rd_sql(server, database, 'TransRefJunctions_evw', version, None, None, None, True, epsg=epsg)

# Transit Frequencies:
if config['build_transit_headways']:
    #df_transit_frequencies = pd.read_csv(os.path.join(data_path, 'transitFrequency.csv'))
    df_transit_frequencies = rd_sql(server, database, 'transitFrequency_evw', version, None, None, None, False, epsg=epsg)