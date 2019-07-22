from osgeo import gdal
import matplotlib.pyplot as plt
from typing import List, Optional, Tuple, Dict, Any
import fnmatch, glob, random, os
from matplotlib.colors import Normalize
from edas.workflow.kernel import EDASDataset
import xarray as xr
import netCDF4 as nc
import numpy as np
from matplotlib.widgets import Slider, Button, RadioButtons
DATA_DIR = os.path.expanduser( "~/DropBox/Tom/Data/WorldClim/MERRA" )

class WorldClimPlotter:

    def plotReference( self, subplot, index: int ):
        tiffFile = f"/Users/tpmaxwel/Dropbox/Tom/Data/WorldClim/MERRA/MERRA_10m_max_00s/10m_max_00s_bio{index}.tif"
        ds = gdal.Open( tiffFile, gdal.GA_ReadOnly )
        rb = ds.GetRasterBand(1)
        img_array: np.ndarray = rb.ReadAsArray()
        subplot.set_title(f'MERRA-Clim Reference: Bio{index}')
        subplot.imshow( img_array, cmap='jet', vmin=img_array.min(), vmax=img_array.max() )

    def plotEDAS( self, subplot, index: int ):
        filePath = os.path.join(DATA_DIR,  "cip_merra2_mth_worldclim_1990.nc")
        fh = nc.Dataset( filePath, mode='r')
        subplot.set_title(f'EDAS WorldClim(MERRA2): Bio{index}')
        for vname in  fh.variables.keys():
            if vname.startswith( f"bio-{index}[" ):
                variable: nc.Variable = fh.variables[vname]
                img_array = np.flip( variable[:], 0 )
                subplot.imshow(img_array, cmap='jet', vmin=img_array.min(), vmax=img_array.max(), aspect=0.8 )

    def plot( self,  wcIndex ):
        subplots = plt.subplots( 2, 2 )
        self.plotReference( subplots[1][0,0], wcIndex )
        self.plotEDAS( subplots[1][1,0], wcIndex )
        self.plotReference(subplots[1][0,1], wcIndex+1 )
        self.plotEDAS(subplots[1][1,1], wcIndex+1 )
        plt.show()

#plotter = WorldClimPlotter()
#plotter.plot(3)

filePath = os.path.join(DATA_DIR,  "cip_merra2_mth_worldclim_1990.nc")
vnames = [ 'bio-1[diff[subset[tempMin]]]', "bio-2[diff[subset[tempMin]]]", "bio-3[diff[subset[tempMin]]]" ]
fh = nc.Dataset(filePath, mode='r')
var: nc.Variable = fh.variables["bio-2[diff[subset[tempMin]]]"]
print( var[ 10:20,10:20] )