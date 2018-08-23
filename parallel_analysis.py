# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 16:27:55 2017

@author: dahaynes
"""

import multiprocessing as mp
import psycopg2, timeit, csv
from postgresql_processing import psqlLib
from copy import deepcopy
from collections import OrderedDict

def GenerateParameters(aDictionary, queryList):

    """
    Generator Function
    """
    for n, query in zip(aDictionary, queryList):
        yield aDictionary["host"], aDictionary["db"], aDictionary["user"], query


def ZonalStats_MergeResults(someResults):
    """

    """                
    finalResults = {}
    for nodeResult in someResults:
        for feature in nodeResult:
            thekey = feature['gid']
            try:
                if feature['gid'] in finalResults.keys():
                    #Still having an issue wth the "min" value
                    finalResults[thekey]["min"] = min( feature["min"], finalResults[thekey]["min"])
                    finalResults[thekey]["max"] = max( feature["max"], finalResults[thekey]["max"])
                    finalResults[thekey]["count"] += feature["count"]
                else:
                    #print("Adding key %s" % (thekey))
                    finalResults[thekey] = { "min": feature["min"], "max" : feature["max"], "count" : feature["count"]}
            except:
                pass
                #print(feature["min"], finalResults[thekey]["min"])
    return finalResults

    
def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    
    connection = psycopg2.connect(database=theConnectionDict['db'], user=theConnectionDict['user'])

    return connection


def NodeQuery(inParameters):
    """
    This is the worker function that runs the query for every node specified    
    """
    import psycopg2
    from psycopg2 import extras
    
    
    connectionDict = inParameters[0]
    sqlQuery = inParameters[1]
    
    conn = CreateConnection(connectionDict)
    
    if conn:
        try:
            cur = conn.cursor(cursor_factory=extras.RealDictCursor)
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

def FeaturePartitionAssignment():
    """
    This function is defunct.
    """
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

    finally:
        pool.close()
        pool.join()
        stop = timeit.default_timer()  
        print("Parallel Query Time %s" % (stop-start) )
        return results

def localDatasetPrep(numNodes=2):
    """

    """
    chunksizes = [50,100, 200, 300, 400, 500, 600, 700, 800]#, 900, 1000, 1500, 2000]#, 2500, 3000, 3500, 4000]
    raster_tables = ["glc_2000_clipped","meris_2015_clipped", "nlcd_2006"] #glc_2000_clipped glc_2010_clipped_400 nlcd_2006_clipped_2500
    
    nodes = ["node%s" % n for n in range(1,numNodes+1)]

    rasterTables =  [ "%s_%s" % (raster, chunk) for raster in raster_tables for chunk in chunksizes  ]
 
    datasetRuns = []
    for r in rasterTables:
        if "glc_2000_clipped" in r: 
            pixelValue = 16
            reclassValues = {"oldPixel" : 16, "newPixel" : 1 }
        elif "meris_2015_clipped" in r:
            pixelValue = 100
            reclassValues = {"oldPixel" : 100, "newPixel" : 1 }
        elif "nlcd_2006" in r:
            pixelValue = 21
            reclassValues = {"oldPixel" : 21, "newPixel" : 1 }
        datasetRuns.append(  OrderedDict([ ("raster_table", r), ("nodes", nodes), ("pixelValue", pixelValue), ("newPixel", 1) ]) )

    return datasetRuns

def zonalDatasetPrep(numNodes=2):
    """
    Function will return all the possible combinations of dastes for analysis
    rasterTables = (raster dataset * tile size) 
    boundaryNames = All boundaries to test against 
    """
    

    chunksizes = [50,100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000]#, 2500, 3000, 3500, 4000]
    raster_tables = ["glc_2000_clipped", "meris_2015_clipped", "nlcd_2006"] #glc_2010_clipped_400 nlcd_2006_clipped_2500
    boundaries = ["states","regions","counties","tracts"]
    nodes = ["node%s" % n for n in range(1,numNodes+1)]

    rasterTables =  [ "%s_%s" % (raster, chunk) for raster in raster_tables for chunk in chunksizes ]
    datasetRuns = []
    for r in rasterTables:
        if "glc_2000_clipped" in r: 
            pixelValue = 16       
        elif "meris_2015_clipped" in r:
            pixelValue = 100
        elif "nlcd_2006" in r:
            pixelValue = 21
        datasetRuns.append(  OrderedDict([ ("raster_table", r), ("nodes", nodes), ("pixelValue", pixelValue) ]) )

    datasetRuns = [ OrderedDict([("boundary_table", "%s_prj" % b),("raster_table", r), ("nodes", nodes)] ) if "nlcd" in r else OrderedDict([("boundary_table", b),("raster_table", r), ("nodes", nodes)] ) for r in rasterTables for b in boundaries ]

    return datasetRuns


def ParallelZonalAnalysis(connectDict, nodeDatasets):
    """
    Function for counting the number of pixels in a geographic feature
    """

    master = psqlLib(connectDict)
    nodeRasterTableIds = master.PartitionRaster(connectDict["raster_table"],len(connectDict["nodes"]) )
    
    #print(nodeRasterTableIds)
    #print(nodeDatasets)
    
    nodeQueries = []
    for n, node in enumerate(nodeDatasets['nodes']):
        
        selectStatement = """
        SELECT p.gid, p.name, (ST_SummaryStatsAgg(ST_Clip(r.rast, p.geom), 1, True)).* 
        FROM {boundary_table} p inner join {raster_table} r on ST_Intersects(r.rast, p.geom) """.format(**nodeDatasets)
        if len(connectDict["nodes"]) > 1:
            whereStatement = """ WHERE r.rid BETWEEN {min} AND {max} """.format(**nodeRasterTableIds[n])
        else:
            whereStatement = ""
        groupStatement = """ GROUP BY p.gid, p.name """
        query = selectStatement.replace("\n","") + whereStatement + groupStatement
        nodeQueries.append(query)   

    return nodeQueries

def ParallelPixelCount(connectDict, nodeDatasets, pixelValue):
    """
    Function for counting the number of pixels in a dataset
    """
    
    master = psqlLib(connectDict)
    nodeRasterTableIds = master.PartitionRaster(connectDict["raster_table"],len(connectDict["nodes"]) )
    
    nodeQueries = []
    for n, node in enumerate(nodeDatasets['nodes']):

        topQuery = """
        With raster_value_count as
        (
        SELECT rid, (ST_ValueCount(rast)).*
        FROM {raster_table}
        """.format(**nodeDatasets)
        
        if len(connectDict["nodes"]) > 1 :
            whereQuery = """
                        WHERE rid BETWEEN {min} AND {max}
                        """.format(**nodeRasterTableIds[n])
        else:
            whereQuery = ""        

        returnQuery = """
        )
        SELECT value, sum(count) as count
        FROM raster_value_count
        WHERE value = %s 
        GROUP BY value
        """ % (pixelValue)
        query = topQuery + whereQuery + returnQuery
        nodeQueries.append(query)

    return nodeQueries

def ParallelReclassification(connectDict, nodeDatasets, pixelValue, reclassValue):
    """
    Function for reclassifying the raster dataset. Returning raster objects.
    """
    
    master = psqlLib(connectDict)
    nodeRasterTableIds = master.PartitionRaster(connectDict["raster_table"],len(connectDict["nodes"]) )
    
    nodeQueries = []
    for n, node in enumerate(nodeDatasets['nodes']):

        reclassQuery = """With analytic as ( SELECT rid, ST_Reclass(rast, 1, '%s:%s', '8BUI', 0) as rast """ % (pixelValue, reclassValue)
        fromQuery = """FROM {raster_table}) SELECT rid FROM analytic """.format(**nodeDatasets)
        if len(connectDict["nodes"]) > 1 :
            whereQuery = """WHERE rid BETWEEN {min} AND {max} """.format(**nodeRasterTableIds[n])
        else:
            whereQuery = ""

        query = reclassQuery + fromQuery + whereQuery
        nodeQueries.append(query) 

    return nodeQueries
            

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

def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Analysis Script for running PostGIS Analytics")   
    parser.add_argument("-n", required =True, type=int, help="Number of PostgreSQL Nodes", dest="nodes", default=2)    
    parser.add_argument("-csv", required =False, help="Output timing results into CSV file", dest="csv", default="None")
    
    subparser = parser.add_subparsers(help='sub-command help', dest="command")
    
   
    analytic_subparser = subparser.add_parser('zonal')
    analytic_subparser.set_defaults(func=zonalDatasetPrep)
    #analytic_subparser.add_argument('-r', required=True, action='store_true', dest='raster')
    #analytic_subparser.add_argument('-v', required=True, action='store_true', dest='vector')

    count_subparser = subparser.add_parser('count')
    #count_subparser.add_argument('-p', required=True, type=int,  help='pixel value', dest="pixelValue")
    count_subparser.set_defaults(func=localDatasetPrep)
    
    reclass_subparser = subparser.add_parser('reclassify')
    #reclass_subparser.add_argument('-old', required=True, type=int,  help='pixel value', dest="oldPixel")
    #reclass_subparser.add_argument('-new', required=True, type=int,  help='pixel value', dest="newPixel")
    reclass_subparser.set_defaults(func=localDatasetPrep)

    return parser


if __name__ == '__main__':
    args = argument_parser().parse_args()
    #print(dir(args))
    testingDatasets = args.func(args.nodes)#datasetprep()
    runs = [1]#,2,3]
    timings = OrderedDict()
    analytic = 0
    filePath = '/home/04489/dhaynes/postgresql_zonal_4_19_2018_4node.csv'
    nodeQueries = []

    for dataset in testingDatasets:
        for r in runs:
            print(dataset)
            start = timeit.default_timer()        

            connectionInfo={"db": "master", "user": "dhaynes", "port": 5432} 
            #The connectionInfo Dictionary contains these items
            #"nodes": ["node1","node2"], "boundary_table": "states", "raster_table": "glc_250"}
            
            for d in dataset:
                connectionInfo[d] = dataset[d]
     
            if args.command == "zonal":
                nodeQueries = ParallelZonalAnalysis(connectionInfo, dataset)
            elif args.command == "count":
                nodeQueries = ParallelPixelCount(connectionInfo, dataset, dataset["pixelValue"])
            elif args.command == "reclassify":
                nodeQueries = ParallelReclassification(connectionInfo, dataset, dataset["pixelValue"], dataset["newPixel"])

            psqlInstances = len(connectionInfo['nodes'])
            #nodeRasterTableIds = {n: {"min": node.min(), "max": node.max() } for n, node in enumerate(nodeQueries) }

            if nodeQueries:
                nodeConnections = []
                for n in connectionInfo['nodes']:
                    nodeConnection = deepcopy(connectionInfo)
                    nodeConnection['db'] = n
                    nodeConnections.append(nodeConnection)


                stopPrep = timeit.default_timer()  
                if args.command == "zonal":
                    results = ParallelQuery(psqlInstances, nodeConnections, nodeQueries)
                    print(nodeQueries)
                else:
                    ParallelQuery(psqlInstances, nodeConnections, nodeQueries)
                #finalstats = ZonalStats_MergeResults(results)
                #print(results)
    
            else:
                print("******* ERROR ******* \n No Records returned in original query")
                quit()

            stop = timeit.default_timer()  
            print("Data Prep Time %s" % (stopPrep-start))
            print("TotalTime %s" % (stop-start))

            analytic += 1
            if args.command == "zonal":
                timings[analytic] = OrderedDict( [("Analytic", args.command), ("run", r), ("numNodes", len(connectionInfo["nodes"]) ), ("raster_table", connectionInfo["raster_table"]), ("boundary_table", connectionInfo["boundary_table"]), ("datapreptime", stopPrep-start), ("querytime", stop-stopPrep)   ])
            else:
                timings[analytic] = OrderedDict( [("Analytic", args.command), ("run", r), ("numNodes", len(connectionInfo["nodes"]) ), ("raster_table", connectionInfo["raster_table"]), ("pixelValue", dataset["pixelValue"]), ("datapreptime", stopPrep-start), ("querytime", stop-stopPrep) ]) 
    WriteFile(filePath, timings)
    print("Finished")

