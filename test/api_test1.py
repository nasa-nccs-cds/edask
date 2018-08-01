import numpy as np
import pandas as pd
import xarray as xr
import time, traceback
from dask.distributed import Client

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

        print( "COMPUTE MEAN, Result:" )

        #    print ds_m.KE.mean(dim='time').mean(dim='lon').mean(dim='lat').values
        print( ds_m.tas.mean().values )

        print( " Completed computation in " + str(time.time() - start) + " seconds" )


    except Exception:
        traceback.print_exc()

    finally:
        print( "SHUTDOWN" )
