import logging, time
import xarray as xr
from dask.distributed import Client, Future, LocalCluster
from edas.config import EdaskEnv
from typing import List, Optional, Tuple, Dict, Any

variable = "t"
appConf = { "sources.allowed": "collection,https", "log.metrics": "true"}
EdaskEnv.update(appConf)

if __name__ == "__main__":
    client = Client( n_workers=4, threads_per_worker=2, memory_limit='2GB' )
    time.sleep(10)
    pathList = "/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/JAN/*.nc"

    start = time.time()
    dset: xr.Dataset = xr.open_mfdataset( pathList, data_vars=[variable], parallel=True )
    da = dset[ variable ]

    da2 = da.groupby('time.month').mean('time')
    da3 = da - da2

    computed_da = da3.load()

    print(computed_da)
