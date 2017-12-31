# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 16:27:55 2017

@author: dahaynes
"""

import multiprocessing as mp
import psycopg2
from psycopg2 import extras

def GetConnectionParameters(aDictionary, queryText,):

    """
    Generator Function
    
    """
    for n in aDictionary['nodes']:
        yield aDictionary['host'], aDictionary['user'], aDictionary['port'], n, queryText
    
    
def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    connection = psycopg2.connect(host=theConnectionDict['host'], database=theConnectionDict['db'], port=theConnectionDict['port'], user=theConnectionDict['user'])
    return connection

def NodeQuery(parameters):
    """
        
    """
       
    print("Here")
   
    connectionDict = {'host': parameters[0], 'user': parameters[1], 'port': parameters[2], 'db': parameters[3] }
    queryText = parameters[4]
    
    conn = CreateConnection(connectionDict)
    
    print("Made connection")
    cur = conn.cursor()
    try:
        cur.execute(queryText)
    except:
        print(cur.query)
        cur.mogrify(queryText)
    else:
        results = cur.fetchall()
        return results
    finally:
        cur.close()
        conn.close()

   

def ParallelQuery(theConnection, theQuery):
    """
    This function creates breaks up a single PostgreSQL query across a variety of Nodes (Postgresql instances)
    """
    
    #import itertools
    
    pool = mp.Pool(len(theConnection['nodes']), maxtasksperchild=1)    #Multiprocessing module
    pool.map(NodeQuery, (i for i in GetConnectionParameters(theConnection, theQuery)) )
#
#    pool.map(test, [1, 2, 3] )        
#    
#    results = pool.join()
if __name__ == '__main__':
    
    
    nodes = [1, 2]
#ParallelQuery("SELECT *", nodes)

    connectionInfo={"host": "localhost", "db": "research", "user": "david", "port": 5432, "nodes": ["research","research"]}
    queryDict = {"boundary_table": "us_states", "srid": 4326, "raster_table": "glc2000_tilesize200", "raster_ids": [3,4,2,5]}
    queryDict["raster_ids"] = str(queryDict["raster_ids"]).replace("[","").replace("]","")
    queryText = """With boundary as 
    (
    SELECT gid as id, name, ST_Transform(p.geom, {srid}) as geom
    FROM {boundary_table} p
    )
    SELECT p.id, p.name, (ST_SummaryStatsAgg(ST_Clip(r.rast, p.geom), 1, True)).*
    FROM boundary p inner join {raster_table} r on ST_Intersects(r.rast, p.geom)
    WHERE r.rid IN ( {raster_ids} )
    GROUP BY p.id, p.name"""
    nQueryText = queryText.format(**queryDict).replace("\n","")
    print(nQueryText)
    
    ParallelQuery(connectionInfo, nQueryText)
#for n in connectionInfo['nodes']:
#    print(n)

#conn = psycopg2.connect(host=connectionInfo['host'], database=connectionInfo['db'], port=connectionInfo['port'], user=connectionInfo['user'])



#'With boundary as ( SELECT gid as id, name, ST_Transform(p.geom, {srid}) as geom FROM {boundary_table} p ) SELECT p.id, p.name, (ST_SummaryStatsAgg(ST_Clip(r.rast, p.geom))).* FROM boundary p inner join %(raster_table)s r on ST_Intersects(r.rast, p.geom)\n    WHERE r.rid = IN (%{raster_ids}s)    GROUP BY p.id'
