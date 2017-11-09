import subprocess, os, itertools
import multiprocessing as mp


def BuildRaster(rasterFilePath , rasterTableName, binaryFilePath, srid=4326, tilesize=250):
    """
    This function will build run the binary program raster2pgsql and save the file
    """

    rasterCommand = "raster2pgsql -C -x -I -Y -F -t %sx%s -s %s %s %s_%s > %s" % (tilesize, tilesize, srid, rasterFilePath, rasterTableName,tilesize, binaryFilePath)
    
    print(rasterCommand)
    p = subprocess.Popen(rasterCommand, stdout=subprocess.PIPE, shell=True)
    p.wait()
    out, err = p.communicate()
    postgisTableName = "%s_%s" % (rasterTableName, tilesize)
    return (binaryFilePath, postgisTableName)

def LoadBinary(inParameters):

    #print(inParameters)
    database =  inParameters[2]
    postgisTableName = inParameters[1]
    binaryFilePath = inParameters[0]
    rasterCommand = "psql -d %s -f %s" % (database, binaryFilePath)
    #print(rasterCommand)
    try:
        p = subprocess.Popen(rasterCommand, stdout=subprocess.PIPE, shell=True)
        p.wait()
        out, err = p.communicate()
    except:
        print("Deleting table %s" % (postgisTableName))
        dropTableCommand = 'psql -d %s -c "drop table %s"' % (database, postgisTableName)
        print(dropTableCommand)
        p = subprocess.Popen(dropTableCommand)
        p.wait()
        #p = subprocess.Popen(rasterCommand, stdout=subprocess.PIPE, shell=True)
        #p.wait()
        #out, err = p.communicate()


def MultiProcessLoading(psqlInstances, binaryFilePath, psqlTableName):
    """
    This function creates the pool based upon the number of SciDB instances and the generates the parameters for each Python instance
    """

    pool = mp.Pool(len(psqlInstances), maxtasksperchild=1)    #Multiprocessing module

    try:
        results = pool.imap(LoadBinary, ((binaryFilePath,psqlTableName, i) for i in psqlInstances) )
    except:
        print(mp.get_logger())

      
        

    pool.close()
    pool.join()
    os.remove(binaryFilePath)


def Main(rasterFilePath, rasterTableName, binaryFilePath, srid=4326, tilesize=250):
    """

    """
    binaryPath, postTableName = BuildRaster(rasterFilePath , rasterTableName, binaryFilePath, srid, tilesize)
    psqlDatabases = ['master', 'node1', 'node2', 'node3', 'node4', 'node5', 'node6', 'node7', 'node8', 'node9', 'node10', 'node11', 'node12']
    if os.path.isfile(binaryPath):
        MultiProcessLoading(psqlDatabases, binaryPath, postTableName)


def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "multiprocessing module for loading dataset into PostGIS with multiple instances")     
    parser.add_argument("-f", required =True, help="raster file path", dest="filepath")
    parser.add_argument("-n", required =True, help="raster table name", dest="tablename")
    parser.add_argument("-b", required =True, help="raster binary file", dest="binarypath")
    parser.add_argument("-s", required =False, help="raster projection", dest="srid", default=4326)
    parser.add_argument("-t", required =False, help="raster tile size", dest="tilesize", default=250)


    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args()
    Main(args.filepath, args.tablename, args.binarypath, args.srid, args.tilesize)
