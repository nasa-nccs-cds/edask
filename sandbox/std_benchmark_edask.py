import numpy as np
import pandas as pd
import xarray as xa
import time, traceback
from dask.distributed import Client

print ( "STARTUP" )
client = None
start = time.time()

edas_cip_tas_6hr="/dass/dassnsd/data01/cldra/data/pubrepo/CREATE-IP/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/6hr/atmos/tas/*.nc"
edas_cip_tas_mon="/dass/dassnsd/data01/cldra/data/pubrepo/CREATE-IP/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos/tas/*.nc"
dataset = edas_cip_tas_mon

try:
    client = Client( 'edaswndev01:8786' )
    print( "READ " + dataset )
    
    ds_m=xa.open_mfdataset( dataset, data_vars=['tas'], parallel=True )

    print( "COMPUTE MEAN, Result:" )

    print( ds_m.tas.mean( dim=["lat","lon"], keep_attrs=True ).values )

    print( " Completed computation in " + str(time.time() - start) + " seconds" )


except Exception:
    traceback.print_exc()

finally:
    print( "SHUTDOWN" )
    if client: client.close()
