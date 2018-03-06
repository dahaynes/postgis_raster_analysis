# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 16:27:55 2017

@author: dahaynes
"""

import multiprocessing as mp
import psycopg2, timeit, csv
from postgresql_processing import postgis
from copy import deepcopy
from collections import OrderedDict

def GenerateParameters(aDictionary, queryList):

    """
    Generator Function
    """
    for n, query in zip(aDictionary, queryList):
        yield aDictionary["host"], aDictionary["db"], aDictionary["user"], query


def GroupRecordsByNode(partitionedRecords): 
    """
    This function will takes the partiioned records and groups their information by node.
    """   
    nodeRecords = {}
    for group in partitionedRecords.keys():
        placeNames = []
        placeRasterIds = []    
        for rec in partitionedRecords[group].keys():
            placeNames.append("'%s'" % (partitionedRecords[group][rec]["name"]))
            placeRasterIds.append(partitionedRecords[group][rec]["raster_ids"])
            
        nodeRecords[group] = {"name": placeNames, "raster_ids": placeRasterIds}

    return nodeRecords

def CreateNodeQueries(partitionedQuery, nodeRecords, dataTables):
    """
    This function will create the paritioned query that you need to send to each node.
    """
    nodeData = {}
    for node in nodeRecords:
        nameString = ",".join(nodeRecords[node]["name"])
        rasterIDString = ",".join(nodeRecords[node]["raster_ids"])
        uniqueID = ",".join(list(set(rasterIDString.split(","))))
        nodeData[node] = {"place_names": nameString, "raster_ids": uniqueID}

        for d in dataTables.keys():
            # print(d)
            nodeData[node][d] = dataTables[d]
     
    queries = [partitionedQuery.format( **nodeData[n]).replace("\n","") for n in nodeData]

    return queries
    
    
def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    
    connection = psycopg2.connect(database=theConnectionDict['db'], user=theConnectionDict['user'])

    return connection


def ThreadedConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    pooledConnect = pool.PersistentConnectionPool(1, 5, host=theConnectionDict['host'], database=theConnectionDict['db'], port=theConnectionDict['port'], user=theConnectionDict['user'])
    
    return pooledConnect

def NodeQuery(inParameters):
    """
    This is the worker function that runs the query for every node specified    
    """
    
    connectionDict = inParameters[0]
    sqlQuery = inParameters[1]
    conn = CreateConnection(connectionDict)
    
    if conn:
        try:
            cur = conn.cursor()
            # cur = conn.cursor(cursor_factory=extras.RealDictCursor)
            cur.execute(sqlQuery)

        except psycopg2.Error as e:
            print(e.pgerror)
        else:
            return cur.fetchall()

        finally:
            cur.close()
            conn.close()
    else:
        print("No Connection Made")

   

def ParallelQuery(psqlInstances, theConnectionInfos, theQueries):
    """
    This function takes a pre-partitioned PostgreSQL and uses the multiprocessing module
    to send the query across a variety of Postgresql instances.
    """
    start = timeit.default_timer() 

    try:
        pool = mp.Pool(psqlInstances, maxtasksperchild=1)  
        results = pool.map(NodeQuery, (i for i in zip(theConnectionInfos, theQueries)) )
    except:
        
        print(mp.get_logger())
    else:
        pass
        #for r in results:
            #print(r)
    finally:
        pool.close()
        pool.join()
        stop = timeit.default_timer()  
        print("Parallel Query Time %s" % (stop-start) )
        
def datasetprep(numNodes=2):
    """
    Function will return all the possible combinations of dastes for analysis
    rasterTables = (raster dataset * tile size) 
    boundaryNames = All boundaries to test against 
    """
    

    chunksizes = [50,100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000, 2500, 3000, 3500, 4000]
    raster_tables = ["glc_2000_clipped", "meris_2015_clipped", "nlcd_2006_clipped"] #glc_2010_clipped_400 nlcd_2006_clipped_2500
    boundaries = ["states","regions","counties","tracts"]
    nodes = ["node%s" % n for n in range(1,numNodes+1)]

    rasterTables =  [ "%s_%s" % (raster, chunk) for raster in raster_tables for chunk in chunksizes ]        

    datasetRuns = [ OrderedDict([("boundary_table", "%s_proj" % b),("raster_table", r), ("nodes", nodes)] ) if "nlcd" in r else OrderedDict([("boundary_table", b),("raster_table", r), ("nodes", nodes)] ) for r in rasterTables for b in boundaries ]

    return datasetRuns


def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv
    """
    
    thekeys = list(theDictionary.keys())
    
    with open(filePath, 'w') as csvFile:
        fields = list(theDictionary[thekeys[0]].keys())
        theWriter = csv.DictWriter(csvFile, fieldnames=fields)
        theWriter.writeheader()

        for k in theDictionary.keys():
            theWriter.writerow(theDictionary[k])

if __name__ == '__main__':

    testingDatasets = datasetprep()
    runs = [1]#,2,3]
    timings = OrderedDict()
    analytic = 0
    filePath = '/home/04489/dhaynes/postgresql_3_5_2018.csv'

    for dataset in testingDatasets:
        for r in runs:
            print(dataset)
            start = timeit.default_timer()        
            connectionInfo={"db": "master", "user": "dhaynes", "port": 5432} #"nodes": ["node1","node2"], "boundary_table": "states", "raster_table": "glc_250"}
            
            for d in dataset:
                connectionInfo[d] = dataset[d]
     
            m = postgis(connectionInfo)
           
            selectStatement = """SELECT gid, name, replace(replace(array_agg(r.rid)::text, '{', ''), '}', '') as raster_ids"""
            fromStatement = """FROM {boundary_table} p inner join {raster_table} r on ST_Intersects(r.rast, p.geom) GROUP BY p.gid, name""".format(**connectionInfo)
            query = "%s %s" % (selectStatement, fromStatement)
            records = m.Query(query )
            m.DropConnection()
            jnk ={"raster_ids": '[1,2,3]', "place_names": '[Alaska]'}
            psqlInstances = len(connectionInfo['nodes'])
            p_records = m.PartitionResults(records,psqlInstances)
            nodeRecords = GroupRecordsByNode(p_records)
            # print(nodeRecords)
            queryText = """ SELECT p.gid, p.name, (ST_SummaryStatsAgg(ST_Clip(r.rast, p.geom), 1, True)).* 
            FROM {boundary_table} p inner join {raster_table} r on ST_Intersects(r.rast, p.geom) 
            WHERE r.rid IN ( {raster_ids} ) AND p.name IN ( {place_names} ) 
            GROUP BY p.gid, p.name """
            # print(queryText)
            p_queries = CreateNodeQueries(queryText, nodeRecords, connectionInfo)

            if p_records:
                nodeConnections = []
                for n in connectionInfo['nodes']:
                    nodeConnection = deepcopy(connectionInfo)
                    nodeConnection['db'] = n
                    nodeConnections.append(nodeConnection)


                stopPrep = timeit.default_timer()  
                ParallelQuery(psqlInstances, nodeConnections, p_queries)
            else:
                print("No Records returned in original query")
                print(query)
            
            
            stop = timeit.default_timer()  
            print("Data Prep Time %s" % (stopPrep-start))
            print("TotalTime %s" % (stop-start))

            analytic += 1

            timings[analytic] = OrderedDict( [("connectionInfo", "XSEDE"), ("run", r), ("numNodes", len(connectionInfo["nodes"]) ), ("raster_table", connectionInfo["raster_table"]), ("boundary_table", connectionInfo["boundary_table"])] )
            break
    if filePath: WriteFile(filePath, timings)
    print("Finished")

