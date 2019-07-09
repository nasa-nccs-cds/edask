from osgeo import gdal
import matplotlib.pyplot as plt
import numpy as np

imageFile = "/Users/tpmaxwel/Dropbox/Tom/Data/WorldClim/MERRA/MERRA_10m_max_00s/10m_max_00s_bio18.tif"
ds = gdal.Open( imageFile, gdal.GA_ReadOnly)
rb = ds.GetRasterBand(1)
img_array: np.ndarray = rb.ReadAsArray()
print( img_array.shape )

plt.imshow( img_array, cmap='jet' )
plt.show()

print( "done" )