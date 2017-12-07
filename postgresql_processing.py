# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 08:57:09 2017

@author: dahaynes
"""

class postgis (object):
    
    def __init__(self, connectionDict):
        import psycopg2
        from psycopg2 import extras
        self.psy = psycopg2
        self.connectionDict = connectionDict
        
        try:
            self.connection = self.GetConnection(connectionDict)
            self.cursor = self.connection.cursor(cursor_factory=extras.DictCursor)
            
        except psycopg2.Error as e:
            print(e.pgerror)           
        
        else:
            pass
    
    
    def __del__(self):
        """
        Method to close down the postgresql connection. 
        """
        try:
            if self.connection: self.connection.close() 
        except AttributeError:
            pass
             
    
    def DropConnection(self):
        """
        Method to close down the postgresql connection. 
        """
        self.connection.close()  
        del self.connection
        
        
    def GetConnection(self, theConnectionDict):
        """
        This method will get a connection. Need to make sure that the DB is set correctly.
        """
        connection = self.psy.connect(host=theConnectionDict['host'], database=theConnectionDict['db'], port=theConnectionDict['port'], user=theConnectionDict['user'])
        return connection
    
    def ParallelAnalysis(self, parameters):
        """
        
        """
        print("Here")
        print(parameters)
        #conn = self.GetConnection()
        
    
    def Query(self, theQuery):
        """
        Method of the class to send query and return results
        I return a custom ordered Dictionary, so you can get the keys very easily
        Dictionary looks like
        {recID, {attribute: value, attribute2: value, attribute2: value}}
        """
        from collections import OrderedDict
        
        try:
            self.cursor.execute(theQuery)
        except self.psy.Error as e:
            print(e.pgerror) 
        else:
            records = self.cursor.fetchall()
            
            if len(records) >= 1:
                theKeys = records[0].keys()
                keyNames = [key for key in theKeys]
            
            recDict = OrderedDict()
            for r in records:
                valueDict = {}
                for key in keyNames:
                    valueDict[key] = r[key]
                
                #This is stupid, I didn't want to enumerate
                recDict[records.index(r)] = valueDict
                                    
            return recDict
        
    def PartitionResults(self, results, partitions):
        """
            
        """
        from itertools import cycle
        from math import floor
              
        if partitions > 1:
            
            chunks = floor(len(results)/partitions)
            outDict = {}
            for i in range(0, len(results) - (len(results)%partitions), chunks):
                
                print(i, chunks)
                #Creates the dictionary from the range
                resultsDict = {x:results[x] for x in range(i, i + chunks) if x < len(results)}
                #When range is 0,0 we don't want to return something
                if resultsDict:
                    outDict[i] = resultsDict
                                       
            #outDictLength = {r: len(outDict[r].keys()) for r in outDict }
            
            #theExtraKey, num_records = min(outDictLength.items(), key = lambda t: t[1])
            #if num_records == 0: finish = sum([len(outDict[d].keys()) for d in outDict])
                                          
                                    
            if len(results)%partitions:
                print("Extra")
                num_records = len(results)%partitions
                extraKeys = list(results.keys())[-num_records:]
                
#                #print(outDict.keys())
#                resultsDict = {y:results[y] for y in outDict[theExtraKey]}
#
#                print(len(resultsDict))
#                
                for r, key in zip(extraKeys, cycle(list(outDict.keys())) ):
                    maxKey = max(list(outDict[key].keys()))
                    print(maxKey, r)
                    outDict[key][maxKey+1] = results[r]
#                
#                del outDict[theExtraKey]
#                finish = sum([len(outDict[d].keys()) for d in outDict])
#                print(finish)
#                
#                    
#                #if resultsDict: outDict[c+1] = resultsDict    
#            if start == finish: 
#                print("Awesome")
#            else:
#                print("Failure")
                                       
            return outDict
                
        else:
            return results
    
    def PickleParameters(self, query, db):

        """
        Generator for the class
        Input: 
        scidbInstance = SciDB Instance IDs
        rasterFilePath = Absolute Path to the GeoTiff
        """
        import itertools
        
        for query, n in zip(db, itertools.repeat(query)):
            yield query, n
    
    def ParallelAnalysis(parameters):
        """
        
        """
        print("Here")
        print(parameters)
        #conn = self.GetConnection()

    def ParallelQuery(self, theQuery ):
        """
        This function creates breaks up a single PostgreSQL query across a variety of Nodes (Postgresql instances)
        """
     
        import multiprocessing as mp
        import itertools
        
        pool = mp.Pool(len(self.connectionDict['nodes']), maxtasksperchild=1)    #Multiprocessing module
                    
        
        pool.map(ParallelAnalysis, [1,2] )
        pool.close()
        pool.join()

             
#            if results:
#                print(results, dir(results))
#                timeDictionary  = {str(i[0]):{"version": i[0], "writeTime": i[1], "loadTime": i[2] } for i in results}
#    
#                return timeDictionary    




connectionInfo={"host": "localhost", "db": "research", "user": "david", "port": 5432, "nodes": [1,2]}

m = postgis(connectionInfo)

query = """With boundary as 
(
SELECT gid as id, name, ST_Transform(p.geom, 5070) as geom
FROM states p
)

SELECT p.id, array_agg(r.rid)
FROM boundary p inner join nlcd_2006 r on ST_Intersects(r.rast, p.geom)
GROUP BY p.id"""

records = m.Query("SELECT * FROM raster_columns")
dataset = m.PartitionResults(records, 2)
m.DropConnection()
m.ParallelQuery("SELECT * FROM raster_columns")



