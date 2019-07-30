from osgeo import gdal
import matplotlib.pyplot as plt
import fnmatch, glob, random
from matplotlib.colors import Normalize
import xarray as xr
import netCDF4 as nc
import numpy as np
wcIndex = 1
np.random.seed(123)

class WorldClimPlotter:

    def plotReference( self, subplot, index: int ):
        tiffFile = f"/Users/tpmaxwel/Dropbox/Tom/Data/WorldClim/MERRA/MERRA_10m_max_00s/10m_max_00s_bio{index}.tif"
        ds = gdal.Open( tiffFile, gdal.GA_ReadOnly )
        rb = ds.GetRasterBand(1)
        img_array: np.ndarray = rb.ReadAsArray()
        subplot.set_title(f'MERRA-Clim Reference: Bio{index}')
        subplot.imshow( img_array, cmap='jet', vmin=img_array.min(), vmax=img_array.max() )

    def plotEDAS( self, subplot, index: int ):
        filePath = "/Users/tpmaxwel/Dropbox/Tom/Data/WorldClim/MERRA/merra2-WorldClim-mean-1990-2000.nc"
        fh = nc.Dataset( filePath, mode='r')
        subplot.set_title(f'EDAS WorldClim(MERRA2): Bio{index}')
        for vname in  fh.variables.keys():
            if vname.startswith( f"bio{index}[" ):
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

class NetcdfPlotter:

    def plotVariables(self ):
        subplots = plt.subplots(2, 2)
        self.plotVariable( subplots[1][0,0], random.randint(1,31), random.randint(1,23) )
        self.plotVariable( subplots[1][0,1], random.randint(1,31), random.randint(1,23) )
        self.plotVariable( subplots[1][1,0], random.randint(1,31), random.randint(1,23) )
        self.plotVariable( subplots[1][1,1], random.randint(1,31), random.randint(1,23) )

    def plotVariablesMax(self ):
        subplots = plt.subplots(2, 2)
        self.plotVariableMax( subplots[1][0,0], random.randint(1,31) )
        self.plotVariableMax( subplots[1][0,1], random.randint(1,31) )
        self.plotVariableMax( subplots[1][1,0], random.randint(1,31) )
        self.plotVariableMax( subplots[1][1,1], random.randint(1,31) )

    def plotVariable( self, subplot, day: int, hour: int ):
        filePath = f"/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/inst1_2d_asm_Nx.2018-7/MERRA2_400.inst1_2d_asm_Nx.201807{day:02}.nc4.nc?T2M[0:23][48:215][134:251],QV2M[0:23][48:215][134:251],lat[48:215],time[0:23],lon[134:251]"
        vname = "QV2M"
        fh = nc.Dataset(filePath, mode='r')
        variable: nc.Variable = fh.variables[vname]
        img_array = np.flip(variable[hour,:,:], 0)
        subplot.set_title(f"QV2M: day: {day}, hour: {hour}")
        subplot.imshow(img_array, cmap='jet', vmin=img_array.min(), vmax=img_array.max(), aspect=0.8)

    def plotSingleVariable(self, filePath, vname ):
        fh = nc.Dataset(filePath, mode='r')
        variable: nc.Variable = fh.variables[vname]
        img_array = variable[:,:]
        plt.imshow(img_array, cmap='jet', aspect=0.8)

    def plotVariableMax( self, subplot, day: int ):
        filePath = f"/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/inst1_2d_asm_Nx.2018-7/MERRA2_400.inst1_2d_asm_Nx.201807{day:02}.nc4.nc?T2M[0:23][48:215][134:251],QV2M[0:23][48:215][134:251],lat[48:215],time[0:23],lon[134:251]"
        vname = "QV2M"
        fh = xr.open_dataset(filePath)
        variable: xr.Variable = fh.variables[vname]
        data_max: xr.DataArray = variable.max( dim="time")
        img_array = np.flip(data_max.values, 0)
        subplot.set_title(f"QV2M max: day: {day}")
        subplot.imshow(img_array, cmap='jet', vmin=img_array.min(), vmax=img_array.max(), aspect=0.8)


file = "/tmp/endpoint-sample-result-0.nc"
var = "tas-time-ave"

plotter = NetcdfPlotter()

plotter.plotSingleVariable(file,var)

plt.show()
