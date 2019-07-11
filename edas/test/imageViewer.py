from osgeo import gdal
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize
import netCDF4 as nc
import numpy as np
wcIndex = 1

class WorldClimPlotter:

    def plotReference( self, subplot, index: int ):
        tiffFile = f"/Users/tpmaxwel/Dropbox/Tom/Data/WorldClim/MERRA/MERRA_10m_max_00s/10m_max_00s_bio{index}.tif"
        ds = gdal.Open( tiffFile, gdal.GA_ReadOnly )
        rb = ds.GetRasterBand(1)
        img_array: np.ndarray = rb.ReadAsArray()
        subplot.set_title(f'MERRA-Clim Reference: Bio{index}')
        subplot.imshow( img_array, cmap='jet', vmin=img_array.min(), vmax=img_array.max() )

    def plotEDAS( self, subplot, index: int ):
        filePath = "/Users/tpmaxwel/Dropbox/Tom/Data/WorldClim/MERRA/merra2-WorldClim-mean-1990-1991.nc"
        fh = nc.Dataset( filePath, mode='r')
        subplot.set_title(f'EDAS WorldClim(MERRA2): Bio{index}')
        for vname in  fh.variables.keys():
            if vname.startswith( f"bio{index}[" ):
                variable: nc.Variable = fh.variables[vname]
                img_array = np.flip( variable[:], 0 )
                subplot.imshow(img_array, cmap='jet', vmin=img_array.min(), vmax=img_array.max(), aspect=0.8 )

    def plot( self,  wcIndex = 0 ):
        if wcIndex > 0:
            subplots = plt.subplots( 2, 1 )
            self.plotReference( subplots[1][0], wcIndex )
            self.plotEDAS( subplots[1][1], wcIndex )
        else:
            subplots = plt.subplots( 19, 2 )
            plt.figure(num=1, figsize=( 10, 60 ), dpi=80)
            for wci in range(1,19):
                self.plotReference(subplots[1][wci,0], wci)
                self.plotEDAS(subplots[1][wci,1], wci)
        plt.show()

plotter = WorldClimPlotter()
plotter.plot(11)
