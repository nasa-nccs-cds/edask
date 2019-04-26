import numpy as np
import pandas as pd
import xarray as xa
import time, traceback
from dask.distributed import Client

print ( "STARTUP" )
client = None
dataset_month = '/dass/dassnsd/data01/cldra/data/pubrepo/MERRA2/M2I1NXINT.5.12.4/1980/01/*.nc4'
dataset_year = '/dass/dassnsd/data01/cldra/data/pubrepo/MERRA2/M2I1NXINT.5.12.4/1980/*/*.nc4'
dataset_35year = '/dass/dassnsd/data01/cldra/data/pubrepo/MERRA2/M2I1NXINT.5.12.4/*/*/*.nc4'
scheduler_node = "edaskwndev01"
# scheduler_node = "cldradn101"
dataset = dataset_year

try:
    client = Client( scheduler_node + ':8786' )

    print( "READ " + dataset )
    start = time.time()
    ds_m=xa.open_mfdataset( dataset, autoclose=True, data_vars=['KE'], parallel=True )
    result = ds_m.KE.mean().values
    print( " Completed computation in " + str(time.time() - start) + " seconds" )

    print( "COMPUTE MEAN, Result:" )
    print( ds_m.KE.mean().values )

except Exception:
    traceback.print_exc()

finally:
    print( "SHUTDOWN" )
    if client: client.close()
