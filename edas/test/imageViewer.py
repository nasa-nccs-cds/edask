from osgeo import gdal
import matplotlib.pyplot as plt
import netCDF4 as nc
import numpy as np

def view_geotiff( index: int ):
    tiffFile = f"/Users/tpmaxwel/Dropbox/Tom/Data/WorldClim/MERRA/MERRA_10m_max_00s/10m_max_00s_bio{index}.tif"
    ds = gdal.Open( tiffFile, gdal.GA_ReadOnly )
    rb = ds.GetRasterBand(1)
    img_array: np.ndarray = rb.ReadAsArray()
    print(img_array.shape)
    plt.imshow(img_array, cmap='jet')
    plt.show()

def view_netcdf( index: int ):
    filePath = "/Users/tpmaxwel/merra-worldClim.nc"
    fh = nc.Dataset( filePath, mode='r')
    for vname in  fh.variables.keys():
        print( vname )
        if vname.startswith( f"bio{index}[" ):
            variable: nc.Variable = fh.variables[vname]
            img_array = np.flip( variable[:], 0 )
            plt.imshow(img_array, cmap='jet')
            plt.show()

view_geotiff( 2 )
# view_netcdf( 2 )

print( "done" )