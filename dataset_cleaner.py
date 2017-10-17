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


inGeoTiffPath = r"c:\scidb\glc2000.tif"
outGeoTiffPath = r"c:\scidb\glc2000_clean.tif"

r = gdal.Open(inGeoTiffPath)
#print(dir(r))
rasterBand = r.GetRasterBand(1)

write_new_raster(r.RasterXSize, r.RasterYSize, r.GetProjection(), r.GetGeoTransform(), rasterBand.ReadAsArray(), outGeoTiffPath, rasterBand.DataType, 255, 255)
print("Done")