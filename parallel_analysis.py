# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 16:27:55 2017

@author: dahaynes
"""

import multiprocessing as mp
import psycopg2, timeit
from psycopg2 import extras
from postgresql_processing import postgis

def GenerateParameters(aDictionary, queryText, placeName):

    """
    Generator Function
    """
    import itertools

    for query, name, nodes in zip(queryText, placeName, itertools.cycle(aDictionary['nodes']) ):
        yield aDictionary['host'], aDictionary['user'], aDictionary['port'], nodes, query, name
    
    
def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    connection = psycopg2.connect(host=theConnectionDict['host'], database=theConnectionDict['db'], port=theConnectionDict['port'], user=theConnectionDict['user'])
    return connection

def NodeQuery(parameters):
    """
    This is the worker function that runs the query for every node specified    
    """
    
    connectionDict = {'host': parameters[0], 'user': parameters[1], 'port': parameters[2], 'db': parameters[3] }
    queryText = parameters[4]
    place_name = parameters[5]
    
    conn = CreateConnection(connectionDict)
    
    #print("Made connection with databases %s" % ( parameters[3]) )
    cur = conn.cursor(cursor_factory=extras.RealDictCursor)
    try:
        cur.execute(queryText)
    except:
        print("ERROR", cur.query)
    else:
        for rec in cur:
            #print(rec)
            if rec['name'] == place_name:
                value = rec
                cur.close()
                conn.close()
                return value
    finally:
        cur.close()
        conn.close()

   

def ParallelQuery(theConnection, theQueries, thePlaces):
    """
    This function takes a pre-partitioned PostgreSQL and uses the multiprocessing module
    to send the query across a variety of Postgresql instances.
    """
    start = timeit.default_timer()  
    try:
        pool = mp.Pool(len(theConnection['nodes']), maxtasksperchild=1)  
        results = pool.map(NodeQuery, (i for i in GenerateParameters(theConnection, theQueries, thePlaces)) )
    except:
        print(mp.get_logger())
    else:
        for r in results:
            print(r)
    finally:
        pool.close()
        pool.join()
        stop = timeit.default_timer()  
        print("Parallel Query Time %s" % (stop-start) )

if __name__ == '__main__':
    start = timeit.default_timer()        
    connectionInfo={"host": "localhost", "db": "master", "user": "david", "port": 5432, "nodes": ["node1","node2"], "boundary_table": "states", "raster_table": "glc_250"}
    m = postgis(connectionInfo)

    query = """SELECT p.id, name, replace(replace(array_agg(r.rid)::text, '{', ''), '}', '') as raster_ids FROM states p inner join glc_250 r on ST_Intersects(r.rast, p.geom) GROUP BY p.id, name"""
    records = m.Query(query)
    m.DropConnection()


    #connectionInfo={"host": "localhost", "db": "research", "user": "david", "port": 5432, "nodes": ["research","research"]}
    #queryDict = {"boundary_table": "us_states", "srid": 4326, "raster_table": "glc2000_tilesize200", "raster_ids": [3,4,2,5]}
    #queryDict["raster_ids"] = str(queryDict["raster_ids"]).replace("[","").replace("]","")
    if records:
        queryText = """ SELECT p.id, p.name, (ST_SummaryStatsAgg(ST_Clip(r.rast, p.geom), 1, True)).*
        FROM {boundary_table} p inner join {raster_table} r on ST_Intersects(r.rast, p.geom)
        WHERE r.rid IN ( {raster_ids} ) 
        GROUP BY p.id, p.name """
    
        queries = [queryText.format(**connectionInfo, **records[r]).replace("\n","") for r in records]
        names = [records[r]['name']for r in records]
            
        #nQueryText = queryText.format(**queryDict).replace("\n","")
    
        print(queries[0])
        stopPrep = timeit.default_timer()  
        ParallelQuery(connectionInfo, queries, names)
    else:
        print("No Records returned in original query")
        print(query)
    
    stop = timeit.default_timer()  
    print("Data Prep Time %s" % (stopPrep-start))
    print("TotalTime %s" % (stop-start))
