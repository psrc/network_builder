import os, pyodbc, sqlalchemy, time
from pandas import read_sql
from shapely import wkt
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
import pandas as pd
import os
import numpy as np
import yaml
from log_controller import timed
import time
import sys

#def rd_sql(server, database, table, version,  col_names=None, where_col=None, where_val=None, geo_col=False, epsg=2193, export=False, path='save.csv'):
#    """
#    Imports data from MSSQL database, returns GeoDataFrame. Specific columns can be selected and specific queries within columns can be selected. Requires the pymssql package, which must be separately installed.
#    Arguments:
#    server -- The server name (str). e.g.: 'SQL2012PROD03'
#    database -- The specific database within the server (str). e.g.: 'LowFlows'
#    table -- The specific table within the database (str). e.g.: 'LowFlowSiteRestrictionDaily'
#    col_names -- The column names that should be retrieved (list). e.g.: ['SiteID', 'BandNo', 'RecordNo']
#    where_col -- The sql statement related to a specific column for selection (must be formated according to the example). e.g.: 'SnapshotType'
#    where_val -- The WHERE query values for the where_col (list). e.g. ['value1', 'value2']
#    geo_col -- Is there a geometry column in the table?
#    epsg -- The coordinate system (int)
#    export -- Should the data be exported
#    path -- The path and csv name for the export if 'export' is True (str)
#    """
#    if col_names is None and where_col is None:
#        stmt1 = 'SELECT * FROM ' + table
#    elif where_col is None:
#        stmt1 = 'SELECT ' + str(col_names).replace('\'', '"')[1:-1] + ' FROM ' + table
#    else:
#        stmt1 = 'SELECT * FROM ' + table + ' WHERE ' + str([where_col]).replace('\'', '"')[1:-1] + ' IN (' + str(where_val)[1:-1] + ')'
#    conn = connect(server, database=database)
#    cursor = conn.cursor()
#    cursor.execute("sde.set_current_version %s", version)
#    df = read_sql(stmt1, conn)
	

#    ## Read in geometry if required
#    if geo_col:
#        geo_col_stmt = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=" + "\'" + table + "\'" + " AND DATA_TYPE='geometry'"
#        geo_col = str(read_sql(geo_col_stmt, conn).iloc[0,0])
#        if where_col is None:
#            stmt2 = 'SELECT ' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + table
#        else:
#            stmt2 = 'SELECT ' + geo_col + '.STGeometryN(1).ToString()' + ' FROM ' + table + ' WHERE ' + str([where_col]).replace('\'', '"')[1:-1] + ' IN (' + str(where_val)[1:-1] + ')'
#        df2 = read_sql(stmt2, conn)
#        df2.columns = ['geometry']
#        try:
#            geometry = df2['geometry'].apply(wkt.loads)
#        #geometry2 = map(lambda x: loads(x), test[2].geometry)
#        #geometry = map(loads, df2.geometry)
#        #geometry = [loads(x) for x in df2.geometry]
#        except:
#            print ('Loading Geometry for %s failed. Check to make there are no 0 length features or feature that contain M values. Exiting Program!' % table)

#        df = GeoDataFrame(df, geometry=geometry, crs={'init' :'epsg:' + str(epsg)})
#        if 'Shape' in df.columns:
#            df.drop(['Shape'], axis = 1, inplace = True)

#    if export:
#        df.to_csv(path, index=False)

#    conn.close()
#    return(df)


def read_from_sde(connection_string, feature_class_name, version,
                  crs={'init': 'epsg:2285'}, is_table = False):
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

    engine = sqlalchemy.create_engine(connection_string)
    con=engine.connect()
    con.execute("sde.set_current_version {0}".format(version))
    if is_table:
        gdf=pd.read_sql('select * from %s' % 
                   (feature_class_name), con=con)
        con.close()

    else:
        df=pd.read_sql('select *, Shape.STAsText() as geometry from %s' % 
                   (feature_class_name), con=con)
        con.close()

        df['geometry'] = df['geometry'].apply(wkt.loads)
        gdf=gpd.GeoDataFrame(df, geometry='geometry')
        gdf.crs = crs
        cols = [col for col in gdf.columns if col not in 
                ['Shape', 'GDB_GEOMATTR_DATA', 'SDE_STATE_ID']]
        gdf = gdf[cols]
    
    return gdf

config = yaml.safe_load(open("config.yaml"))

data_path = config['data_path']
model_year = config['model_year']
#server = config['server']
#database = config['database']
connection_string = 'mssql+pyodbc://AWS-PROD-SQL\Sockeye/OSMTest?driver=SQL Server?Trusted_Connection=yes'
version = config['version']
crs = config['crs']
epsg = config['epsg']

# modeAttributes
df_modeAttributes = read_from_sde(connection_string, 'modeAttributes_evw', version, crs=crs, is_table=True)

## Tolls
df_tolls = read_from_sde(connection_string, 'modeTolls_evw', version, crs=crs, is_table=True)
df_tolls = df_tolls[df_tolls['InServiceDate'] == model_year]
df_tolls = df_tolls[config['toll_columns'] + config['dir_toll_columns']]

# Edges
gdf_TransRefEdges = read_from_sde(connection_string, 'TransRefEdges_evw', version, crs=crs, is_table = False)
gdf_TransRefEdges = gdf_TransRefEdges[gdf_TransRefEdges.length > 0]


gdf_TransRefEdges = gdf_TransRefEdges.merge(df_modeAttributes, how = 'left', on = 'PSRCEdgeID')

gdf_TransRefEdges = gdf_TransRefEdges.merge(df_tolls, how = 'left', on = 'PSRCEdgeID')
gdf_TransRefEdges.fillna(0, inplace = True)


## TransitLines
gdf_TransitLines = read_from_sde(connection_string, 'TransitLines_evw', version, crs=crs, is_table = False)
gdf_TransitLines = gdf_TransitLines[gdf_TransitLines.InServiceDate==model_year]

### TransitPoints
gdf_TransitPoints = read_from_sde(connection_string, 'TransitPoints_evw', version, crs=crs, is_table = False)
gdf_TransitPoints = gdf_TransitPoints[gdf_TransitPoints.LineID.isin(gdf_TransitLines.LineID)]

### Projects
if config['update_network_from_projects']:
    #gdf_ProjectRoutes = gpd.read_file(os.path.join(data_path, 'ProjectRoutes.shp'))
    gdf_ProjectRoutes = read_from_sde(connection_string, 'ProjectRoutes_evw', version, crs=crs, is_table = False)
    gdf_ProjectRoutes = gdf_ProjectRoutes[gdf_ProjectRoutes['version'] == config['projects_version_year']]
    gdf_ProjectRoutes['FacilityType'] = gdf_ProjectRoutes['Change_Type'].astype(int)
    
    #scenarios:
    df_tblProjectsInScenarios = read_from_sde(connection_string, 'tblProjectsInScenarios_evw', version, crs=crs, is_table = True)
    df_tblProjectsInScenarios = df_tblProjectsInScenarios[df_tblProjectsInScenarios['ScenarioName']==config['scenario_name']]
    gdf_ProjectRoutes = gdf_ProjectRoutes[gdf_ProjectRoutes['intProjID'].isin(df_tblProjectsInScenarios['intProjID'])]

    # project attributes
    df_tblLineProjects = read_from_sde(connection_string, 'tblLineProjects_evw', version, crs=crs, is_table = True)
    df_tblLineProjects = df_tblLineProjects[df_tblLineProjects.projRteID.isin(gdf_ProjectRoutes.projRteID)]

    # point events (park and rides)
    df_evtPointProjectOutcomes = read_from_sde(connection_string, 'evtPointProjectOutcomes', version, crs=crs, is_table=True)

    gdf_ProjectRoutes = gdf_ProjectRoutes.merge(df_tblLineProjects, how = 'left', on = 'projRteID')
    gdf_ProjectRoutes = gdf_ProjectRoutes.loc[gdf_ProjectRoutes['InServiceDate'] <= config['model_year']]
    # drop InServiceDate as it is on edges
    #gdf_ProjectRoutes.drop(['InServiceDate'], axis = 1, inplace = True)

else:
    gdf_ProjectRoutes = None
    df_tblProjectsInScenarios = None
    df_evtPointProjectOutcomes = None


## Turns
gdf_TurnMovements = read_from_sde(connection_string, 'TurnMovements_evw', version, crs=crs, is_table = False)
gdf_TurnMovements = gdf_TurnMovements[gdf_TurnMovements['InServiceDate'] <= config['model_year']]

## Juncions
gdf_Junctions = read_from_sde(connection_string, 'TransRefJunctions_evw', version, crs=crs, is_table = False)

# Transit Frequencies:
if config['build_transit_headways']:
    #df_transit_frequencies = pd.read_csv(os.path.join(data_path, 'transitFrequency.csv'))
    df_transit_frequencies = read_from_sde(connection_string, 'transitFrequency_evw', version, crs=crs, is_table = True)