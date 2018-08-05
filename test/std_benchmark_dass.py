import numpy as np
import pandas as pd
import xarray as xr
import time, traceback
from dask.distributed import Client

print ( "STARTUP" )
client = None
dataset_month = '/pubrepo/MERRA2/M2I1NXINT.5.12.4/1980/01/*.nc4'
dataset_year = '/pubrepo/MERRA2/M2I1NXINT.5.12.4/1980/*/*.nc4'
dataset_35year = '/pubrepo/MERRA2/M2I1NXINT.5.12.4/*/*/*.nc4'
dataset_ncml = '/dass/adm/edas/cache/collections/agg/merra2_inst1_2d_int_Nx-M2I1NXINT.5.12.4MOfZ.ncml'
start = time.time()

dataset = dataset_35year

try:
    client = Client( 'cldradn101:8786' )

    print( "READ " + dataset )
    
    ds_m=xr.open_mfdataset( dataset, autoclose=True, data_vars=['KE'], parallel=True ) 

    print( "COMPUTE MEAN, Result:" )

#    print ds_m.KE.mean(dim='time').mean(dim='lon').mean(dim='lat').values
    print( ds_m.KE.mean().values )

    print( " Completed computation in " + str(time.time() - start) + " seconds" )


except Exception:
    traceback.print_exc()

finally:
    print( "SHUTDOWN" )
    if client: client.close()
