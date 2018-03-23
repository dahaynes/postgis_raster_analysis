# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 08:57:09 2017
Class for paritioning information for a query.
@author: dahaynes
"""
import numpy as np

class psqlLib(object):
    
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

        connection = self.psy.connect(database=theConnectionDict['db'], user=theConnectionDict['user'])

        return connection
            
    
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
                
                # print(i, chunks)
                #Creates the dictionary from the range
                resultsDict = {x:results[x] for x in range(i, i + chunks) if x < len(results)}
                #When range is 0,0 we don't want to return something
                if resultsDict:
                    outDict[i] = resultsDict   
                                    
            if len(results)%partitions:
                # print("Extra")
                num_records = len(results)%partitions
                extraKeys = list(results.keys())[-num_records:]
                
                
                for r, key in zip(extraKeys, cycle(list(outDict.keys())) ):
                    maxKey = max(list(outDict[key].keys()))
                    # print(maxKey, r)
                    outDict[key][maxKey+1] = results[r]#                
                                       
            return outDict
                
        else:
            return results

    def PartitionRaster(self, raster_table, partitions=2):
        """
        This function is very light weight a
        """

        query = """With raster_tiles as
        (
        SELECT rid, split_part(split_part(ST_AsText(ST_ConvexHULL(rast)), ',', 1), ' ', 2) as y
        -- ST_ConvexHULL(rast) as geom,
        FROM %s
        ORDER BY rid
        ), raster_aggregates as
        (
        SELECT array_agg(rid) as raster_ids
        FROM raster_tiles
        GROUP by y
        )
        SELECT raster_ids[1] as min_tile_id , raster_ids[array_length(raster_ids, 1)] as max_tile_id
        FROM raster_aggregates
        ORDER by raster_ids[1]""" % (raster_table)
        #print(query)

        #self.connection = self.GetConnection(connectionDict)
        cur = self.connection.cursor()
        cur.execute(query)
        dataset = cur.fetchall()

        minTileParitionIds = [i[0] for i in dataset]
        maxTileParitionIds = [i[1] for i in dataset]

        #print(minTileParitionIds, maxTileParitionIds)

        #t = np.column_stack((minTileParitionIds,maxTileParitionIds) )
        #tileDataset = np.rec.array([minTileParitionIds, maxTileParitionIds], dtype=[('minTile','i8'),('maxTile','i8')])
        #Can't put these into a single
        min_max_Ids = np.column_stack( (minTileParitionIds,maxTileParitionIds) )
        
        nodePartitionIds = np.array_split( min_max_Ids, partitions)
        
        nodeRasterTableIds = {n: {"min": node.min(), "max": node.max() } for n, node in enumerate(nodePartitionIds) }

        return nodeRasterTableIds



        
                


