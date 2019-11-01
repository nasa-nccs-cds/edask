from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
from stratus.app.client import StratusClient
import logging, os
import xarray as xa
import numpy as np
from stratus.app.core import StratusCore
from edas.config import EdasEnv

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

    slurmScheduler = 'slurm:default'
    daskScheduler = 'explore101:8786'
    appConf = { 'scheduler.address': slurmScheduler }
    EdasEnv.update( appConf )

    variable = "tas"
    uri = 'https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml'

    settings = dict( stratus = dict( type="endpoint", module="edas.stratus.endpoint", object="EDASEndpoint" ) )
    stratus = StratusCore( settings )
    client: StratusClient = stratus.getClient()
    time_range = {"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"}

    requestSpec = dict(
        domain=[ dict(name="d0") ],
        input=[ dict( uri=uri, name=f"{variable}:v0", domain=f"d0" ) ],
        operation=[ dict( name="edas:ave", axis="t", input=f"v0" )  ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data

    v: xa.DataArray = list(dsets[0].values())[0]
    print( v )
    print( v.to_masked_array().tolist() )

    # for index,dset in enumerate(dsets):
    #     fileName = os.path.expanduser(f"~/edas_endpoint_test_result-{index}.nc")
    #     print( f"Got result[{index}]: Saving to file {fileName} " )
    #     dset.to_netcdf( fileName )
    #     print(f"Completed saving dataset[{index}]: Variables = {dset.variables.keys()} ")

    exit(0)

