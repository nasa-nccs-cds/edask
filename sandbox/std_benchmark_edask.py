import numpy as np
import pandas as pd
import xarray as xa
import time, traceback
from dask.distributed import Client, LocalCluster
from multiprocessing import freeze_support

freeze_support()
print ( "STARTUP" )
client = None
start = time.time()
nWorkers=8

edask_cip_tas_6hr="/dass/dassnsd/data01/cldra/data/pubrepo/CREATE-IP/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/6hr/atmos/tas/*.nc"
edask_cip_tas_mon="/dass/dassnsd/data01/cldra/data/pubrepo/CREATE-IP/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos/tas/*.nc"
dataset = edask_cip_tas_mon

try:
#    client = Client( 'edaskwndev01:8786' )
    client = Client( LocalCluster( n_workers=nWorkers ) )

    print( "READ " + dataset )
    
    ds_m=xa.open_mfdataset( dataset, autoclose=True, data_vars=['tas'], parallel=True )

    print( "COMPUTE MEAN, Result:" )

    print( ds_m.tas.mean().values )

    print( " Completed computation in " + str(time.time() - start) + " seconds" )


except Exception:
    traceback.print_exc()

finally:
    print( "SHUTDOWN" )
    if client: client.close()
