import subprocess, os


def BuildRaster(rasterFilePath , rasterTableName, binaryFilePath, srid=4326, tilesize=250):
    """
    This function will build run the binary program raster2pgsql and save the file
    """

    rasterCommand = "raster2pgsql -C -x -I -Y -F -t %sx%s -s %s %s %s_%s > %s" % (tilesize, tilesize, srid, rasterFilePath, rasterFilePath, tilesize, binaryFilePath)

    p = self.subprocess.Popen(rasterCommand, stdout=self.subprocess.PIPE, shell=True)
    p.wait()
    out, err = p.communicate()

    return binaryFilePath

def LoadBinary(database, binaryFilePath):

    rasterCommand = "psql -d %s -f %s" % (database, binaryFilePath)

    p = self.subprocess.Popen(rasterCommand, stdout=self.subprocess.PIPE, shell=True)
    p.wait()
    out, err = p.communicate()


def MultiProcessLoading(psqlInstances, binaryFilePath):
    """
    This function creates the pool based upon the number of SciDB instances and the generates the parameters for each Python instance
    """

    pool = mp.Pool(len(psqlInstances), maxtasksperchild=1)    #Multiprocessing module

    try:
        results = pool.imap(LoadBinary, itertools.cycle(psqlInstances), itertools.repeat(binaryFilePath))
    except:
        print(multiprocessing.get_logger())

        #timeDictionary  = {str(i[0]):{"version": i[0], "writeTime": i[1], "loadTime": i[2] } for i in results}
        return timeDictionary
        

    pool.close()
    pool.join()


def Main(rasterFilePath, rasterTableName, binaryFilePath, srid=4326, tilesize=250)
    """

    """
    binaryPath = BuildRaster(rasterFilePath , rasterTableName, binaryFilePath, srid, tilesize)
    psqlDatabases = ['master', 'node1', 'node2', 'node3', 'node4', 'node5', 'node6', 'node7', 'node8']
    if os.path.isfile():
        MultiProcessLoading(psqlDatabases, binaryPath)