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

class AnimationPlotter:

    def __init__(self, variables: Dict[str,nc.Variable] ):
        self.fig, self.ax = plt.subplots(1,len(variables))
        self.vars = variables
        self.images = {}
        varlist =  list(variables.values())
        plt.subplots_adjust(left=0.25, bottom=0.25)
        axtime = plt.axes([0.25, 0.1, 0.65, 0.03], facecolor='lightgoldenrodyellow' )
        self.time = Slider( axtime, 'Time', 0,varlist[0].shape[0]-1, valinit=0, valstep=1 )
        self.time.on_changed( self.plotVariable )
        self.plotVariable(0)

    def plotVariable( self, index: int ):
        for iax, (vname, var) in enumerate(self.vars.items()):
            img_array = self.images.setdefault( f"{vname}-{index}", np.flip(var[index,:,:],0) )
            self.ax[iax].set_title(f"{vname} month: {index}")
            self.ax[iax].imshow(img_array, cmap='jet', aspect=0.8)


#filePath = os.path.join(DATA_DIR, "cip-merra2-tas-dailyMinMax-monthlyAveStd.nc")
#vnames = [ 'timeResample-ave[timeResample-max[subset[temp]]]', 'timeResample-ave[timeResample-min[subset[temp]]]' ]
#fh = nc.Dataset(filePath, mode='r')
#variables = { vname:fh.variables[vname] for vname in vnames }

filePath = os.path.join(DATA_DIR,  "cip_merra2_mth_worldclim_1990.nc")
vnames = [ 'bio-1[diff[subset[tempMin]]]', "bio-2[diff[subset[tempMin]]]", "bio-3[diff[subset[tempMin]]]" ]
fh = nc.Dataset(filePath, mode='r')

variables = { vname:fh.variables[vname] for vname in vnames }

plotter = AnimationPlotter( variables )
plt.show()





