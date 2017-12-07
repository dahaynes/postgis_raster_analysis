# -*- coding: utf-8 -*-
"""
Created on Thu Dec  7 16:27:55 2017

@author: dahaynes
"""

def ParallelAnalysis(parameters):
        """
        
        """
        print("Here")
        print(parameters)
        #conn = self.GetConnection()
        
def ParallelQuery(theQuery, nodes ):
    """
    This function creates breaks up a single PostgreSQL query across a variety of Nodes (Postgresql instances)
    """
 
    import multiprocessing as mp
    import itertools
    
    pool = mp.Pool(len(nodes), maxtasksperchild=1)    #Multiprocessing module
                
    
    pool.map(ParallelAnalysis, nodes )
        
    #pool.map(self.ParallelAnalysis, zip( [1,2,3], itertools.cycle('a')) )
    #pool.map(self.ParallelAnalysis, self.connectionDict['nodes'])
#    #pool.map(self.ParallelAnalysis, (p for p in self.PickleParameters(theQuery, self.connectionDict['nodes'])) )
#    try:
#        pass
#        #results = pool.map(self.ParallelAnalysis, range(0,2))
#                #GDALReader, (r for r in Rasters.GetMetadata(SciDBInstances, rasterFilePath, SciDBOutPath, SciDBLoadPath, SciDBHost)  ))
#    except:
#        log = mp.get_logger()
#        print(log)
#        
    
    pool.close()
    pool.join()
    
nodes = [1, 2]
ParallelQuery("SELECT *", nodes)