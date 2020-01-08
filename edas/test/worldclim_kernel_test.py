from edas.workflow.modules.edas import WorldClimKernel
import xarray as xa
import numpy as np
import time
import matplotlib
import matplotlib.pyplot as plt
from edas.workflow.data import EDASArray
from edas.process.test import LocalTestManager
from edas.process.manager import ProcessManager, ExecHandler
from edas.config import EdasEnv

appConf = {"sources.allowed": "collection,https", "log.metrics": "true"}
EdasEnv.update(appConf)
processManager = ProcessManager.initManager(EdasEnv.parms)

kernel = WorldClimKernel()

ds_tmin = xa.open_dataset("https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tasmin.ncml")
tasmin = ds_tmin["tasmin"]
tasmin = EDASArray( "tasmin", "d0", tasmin[0:12,:,:].compute() )

ds_tmax = xa.open_dataset("https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tasmax.ncml")
tasmax = ds_tmax["tasmax"]
tasmax = EDASArray( "tasmax", "d0", tasmax[0:12,:,:].compute() )

ds_pr = xa.open_dataset("https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/pr.ncml")
pr = ds_pr["pr"]
pr = EDASArray( "pr", "d0", pr[0:12,:,:].compute() )

results = kernel.computeIndices( tasmin, tasmax, pr, tscale=10.0, hscale=1.0e5 )