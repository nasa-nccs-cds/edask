from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import xarray as xa
from stratus.app.core import StratusCore

if __name__ == "__main__":

    settings = dict( stratus = dict( type="endpoint", module="edas.stratus.endpoint", object="EDASEndpoint" ) )
    stratus = StratusCore( settings )
    client = stratus.getClient()
    time_range = {"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"}
    uri =  mgr.getAddress("merra2", "tas")

    requestSpec = dict(
        domain=[ dict(name="d0", lat=dict(start=20, end=30, system="values"), lon=dict(start=0, end=10, system="values"), time=time_range) ],
        input=[ dict( uri=uri, name=f"tas:v0", domain=f"d0" ) ],
        operation=[ dict( name="edas:ave", axis="xy", input=f"v0" )  ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data
    for index,dset in enumerate(dsets):
        fileName =  f"/tmp/edas_endpoint_test_result-{index}.nc"
        print( f"Got result[{index}]: Saving to file {fileName} " )
        dset.to_netcdf( fileName )

