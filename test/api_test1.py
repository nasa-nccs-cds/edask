import numpy as np
import pandas as pd
import xarray as xr
import time, traceback
from dask.distributed import Client
from xarray.ufuncs import cos

if __name__ == '__main__':
    print( "STARTUP" )
    dataset_path = '/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1/*.nc'
    dataset_ncml = '/Users/tpmaxwel/.edas/cache/collections/agg/giss_r1i1p1-tas_Amon_GISS-E2-H_historical_r1i1p1_1.ncml'
    start = time.time()

    dataset = dataset_path

    try:
        client = Client()

        print( "READ " + dataset )

        ds_m = xr.open_mfdataset(dataset, autoclose=True, data_vars=['tas'], parallel=True)
        variable: xr.DataArray = ds_m["tas"]
        weights: xr.DataArray  = cos( ds_m.coords['lat'] )
        weighted_var = variable * weights
        print( "Var shape:" + str(variable.shape) )
        print( "weighted_var shape:" + str(weighted_var.shape) )
        sum = weighted_var.sum(['time','lon','lat'])
        norm = weights * variable.count( 'lon')  * variable.dims['time']
        mean: xr.DataArray =  sum / norm

        print( "COMPUTE MEAN, Result:" + str( mean.values ) )

        print( " Completed computation in " + str(time.time() - start) + " seconds" )


    except Exception:
        traceback.print_exc()

    finally:
        print( "SHUTDOWN" )
