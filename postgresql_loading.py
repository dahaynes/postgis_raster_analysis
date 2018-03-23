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

def PrepareSQLFile(sqlFilePath):
    """
    
    This reads the existing sql file and removes the unwanted pieces
    The raster2pgsql makes a large text file that uses a number of copy statements.
    I seem to encountering some time out issue, between the multiprocessing and psql using the regular loading process
    """
    
    with open(sqlFilePath, "r+") as fin:
        dataset = fin.readlines()
        fin.seek(0)
        for line in dataset:
            if len(line.split("\t")) == 2:
                fin.write(line)
        fin.truncate()

def LoadSQLFile(inParameters):
    """
    
    Example create table statement
    #CREATE TABLE "glc_garbage" ("rid" serial PRIMARY KEY,"rast" raster,"filename" text);
    """
       
    import psycopg2

    databaseName =  inParameters[2]
    psqlRasterTableName = inParameters[1]
    sqlFilePath = inParameters[0]
    
    conn = psycopg2.connect(dbname=databaseName, user="david")
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS %s" % (psqlRasterTableName))
    
    cur.execute("""CREATE TABLE %s (rid serial PRIMARY KEY, rast raster, filename text);""" % (psqlRasterTableName) ) 
    conn.commit()    
    
    print("Copying file")
    with open(sqlFilePath, "r") as fin:
        cur.copy_from(fin, psqlRasterTableName, sep="\t", columns=("rast", "filename"))
    print("Raster data loaded, adding constraints")
    cur.execute("CREATE INDEX ON %s USING gist (st_convexhull(rast));" % (psqlRasterTableName) )
    cur.execute("ANALYZE %s;" % (psqlRasterTableName) )
    cur.execute("SELECT AddRasterConstraints('','%s','rast',TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,FALSE,TRUE,TRUE,TRUE,TRUE,FALSE); " % psqlRasterTableName)
   
    conn.close()


def MultiProcessLoading(psqlInstances, binaryFilePath, psqlTableName):
    """
    This function creates the pool based upon the number of SciDB instances and the generates the parameters for each Python instance
    """

    pool = mp.Pool(len(psqlInstances), maxtasksperchild=1)    #Multiprocessing module

    try:
        results = pool.imap(LoadSQLFile, ((binaryFilePath,psqlTableName, i) for i in psqlInstances) )
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



iuwrang-c101.uits.indiana.edu(5)$ ./LoadPostgres.sh -s 5070 -r ../ncld_2006.tif -t nlcd_2006
raster2pgsql -C -x -I -Y -F -t 50x50 -s 5070 ../ncld_2006.tif nlcd_2006_50_50 > /data/04489/dhaynes/nlcd_2006_50.sql