import logging, time
import xarray as xr
from dask.distributed import Client
from typing import List, Optional, Tuple, Dict, Any
from dask_jobqueue import SLURMCluster
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

variable = "tas"
uri = 'https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml'

cluster = SLURMCluster( queue="myNodes" )
cluster.adapt( minimum=1, maximum=4, interval="2s", wait_count=500 )
print( "CLUSTER JOB SCRIPT: " + cluster.job_script() )
client = Client( cluster )

t0 = time.time()
dset: xr.Dataset = xr.open_dataset( uri )
da: xr.DataArray = dset['tas']
da2: xr.DataArray = da.groupby('time.month').mean('time')
da_monthly = da2.load()
print(da_monthly)
print( " Completed computation in " + str( time.time() - t0 ) + " seconds")
client.close()
cluster.close()


