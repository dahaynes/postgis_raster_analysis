from osgeo import gdal, ogr
import numpy as np



def world2Pixel(geoMatrix, x, y):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the pixel location of a geospatial coordinate
    """
    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]
    yDist = geoMatrix[5]
    rtnX = geoMatrix[2]
    rtnY = geoMatrix[4]
    pixel = int((x - ulX) / xDist)
    line = int((ulY - y) / xDist)
    
    return (pixel, line)
    

def Pixel2world(geoMatrix, row, col):
    """
    Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
    the x,y location of a pixel location
    """

    ulX = geoMatrix[0]
    ulY = geoMatrix[3]
    xDist = geoMatrix[1]
    yDist = geoMatrix[5]
    rtnX = geoMatrix[2]
    rtnY = geoMatrix[4]
    x_coord = (ulX + (row * xDist))
    y_coord = (ulY - (col * xDist))

    return (x_coord, y_coord)

  
def ClipRaster(inRasterPath,clippedPath, vectorPath):
    '''This function clips the raster to the extent of the polygon '''
    
    vector_dataset = ogr.Open(vectorPath)
    theLayer = vector_dataset.GetLayer()
    geomMin_X, geomMax_X, geomMin_Y, geomMax_Y = theLayer.GetExtent()
    #print(geomMin_X, geomMax_X, geomMin_Y, geomMax_Y) 

    inRaster = gdal.Open(inRasterPath)
    band = inRaster.GetRasterBand(1)
    bandDataType = band.DataType
    
    rasterTransform = inRaster.GetGeoTransform()
    pixel_size = rasterTransform[1]
    
    ulY, ulX = world2Pixel(inRaster.GetGeoTransform(), geomMin_X, geomMax_Y )
    lrY, lrX = world2Pixel(inRaster.GetGeoTransform(), geomMax_X, geomMin_Y )
    #print(ulY, ulX, lrY, lrX)
    
    imageHeight = abs(int(lrX - ulX))
    imageWidth = abs(int(ulY - lrY))

    coordBottomRight = Pixel2world(inRaster.GetGeoTransform(), ulY, ulX)
    coordTopLeft = Pixel2world(inRaster.GetGeoTransform(), lrY, lrX)

    #print(coordBottomRight, coordTopLeft)

    outTransform= [coordBottomRight[0], pixel_size, 0, coordBottomRight[1], 0, rasterTransform[5] ]
    
    #rasterWidth = int((geomMax_X - geomMin_X) / pixel_size)
    #rasterHeight = int((geomMax_Y - geomMin_Y) / pixel_size)
    
    tiffDriver = gdal.GetDriverByName('GTiff')
    clippedRaster = tiffDriver.Create(clippedPath, imageWidth, imageHeight, 1, bandDataType, ['COMPRESS=LZW'])
    #band.DataType
    
    outputArray = inRaster.ReadAsArray(xoff=ulY, yoff=ulX, xsize=imageWidth, ysize=imageHeight)
    
    clippedRaster.SetProjection(inRaster.GetProjection())
    clippedRaster.SetGeoTransform(outTransform)
    
    theBandRast = clippedRaster.GetRasterBand(1)    
    theBandRast.SetNoDataValue(-999)
    theBandRast.WriteArray(outputArray)
    
    del inRaster, clippedRaster


def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "GDAL Python script for clipping raster datasets")
    parser.add_argument("-i", required =True, help="Input file path for the raster", dest="rasterPath")    
    parser.add_argument("-o", required =True, help="Output file path for the raster", dest="clippedPath")    
    parser.add_argument("-s", required =True, help="Input file path for the shapefile", dest="shpPath")    

    return parser

if __name__ == '__main__':
    args = argument_parser().parse_args()
    ClipRaster(args.rasterPath, args.clippedPath, args.shpPath)
