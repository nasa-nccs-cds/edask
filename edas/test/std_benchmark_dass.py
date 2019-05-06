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
dataset = dataset_35year

if __name__ == "__main__":
    try:
    #    client = Client( scheduler_node + ':8786' )

        client = Client( n_workers = 2, threads_per_worker = 2, memory_limit = '1GB' )
        time.sleep(10)
        print( "READ " + dataset )
        start = time.time()
    #    ds_m=xa.open_mfdataset( dataset, engine='netcdf4', data_vars=['KE'], parallel=True )
        ds_m=xa.open_mfdataset( dataset, engine='netcdf4', data_vars=['KE'] )
        result = ds_m.KE.mean().values
        print( " Completed computation in " + str(time.time() - start) + " seconds" )

        print( "COMPUTE MEAN, Result:" )
        print( result )

    except Exception:
        traceback.print_exc()

    finally:
        print( "SHUTDOWN" )
        if client: client.close()
