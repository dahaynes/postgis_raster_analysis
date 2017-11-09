from osgeo import gdal


def write_new_raster(xSize, ySize, rasterProjection, rasterTransformation, theArray, outPath, rasterType, InComing_NoDataValue=-999, OutGoing_NoDataValue=-999):
    ''' '''
    gtiff = gdal.GetDriverByName("GTiff")
    
    outTiff = gtiff.Create(outPath,xSize, ySize, 1, rasterType, options = [ 'COMPRESS=DEFLATE' ] )
    outBand = outTiff.GetRasterBand(1)
    theArray[theArray == InComing_NoDataValue] = OutGoing_NoDataValue
    outBand.WriteArray(theArray)            
    outBand.SetNoDataValue(OutGoing_NoDataValue)
    
    outTiff.SetProjection(rasterProjection)
    outTiff.SetGeoTransform(rasterTransformation)
    outTiff.FlushCache()
    del outTiff
    
    #maskedRasterBandArray = np.ma.masked_equal(rasterBandArray, OutGoing_NoDataValue)
    
    #return maskedRasterBandArray

def main(inGeoTiffPath, outGeoTiffPath):
    """

    """  

    r = gdal.Open(inGeoTiffPath)
    rasterBand = r.GetRasterBand(1)

    write_new_raster(r.RasterXSize, r.RasterYSize, r.GetProjection(), r.GetGeoTransform(), rasterBand.ReadAsArray(), outGeoTiffPath, rasterBand.DataType, 255, 255)
    print("Done")


def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "GDAL Python script for clipping raster datasets")
    parser.add_argument("-i", required =True, help="Input file path for the raster", dest="rasterPath")    
    parser.add_argument("-o", required =True, help="Output file path for the raster", dest="cleanPath")     

    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args()
    main(args.rasterPath, args.cleanPath)