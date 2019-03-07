from stratus_endpoint.handler.base import Task, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
from xarray import Variable
import xarray as xa
from stratus.handlers.core import StratusCore

if __name__ == "__main__":

    settings = dict( stratus = dict( type="endpoint", module="edas.stratus.endpoint", object="EDASEndpoint" ) )
    stratus = StratusCore( settings )
    client = stratus.getClient()
    time_range = {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}
    uri =  mgr.getAddress("merra2", "tas")
    domains = [ f"d{i}" for i in range(4)]

    requestSpec = dict(
        domain=[
            { "name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
              "lon": {"start": 40, "end": 42, "system": "values"},  "time": time_range },
            { "name": "d1", "lat": {"start": 60, "end": 65, "system": "values"},
              "lon": {"start": 40, "end": 42, "system": "values"}, "time": time_range },
            { "name": "d2", "lat": {"start": 50, "end": 55, "system": "values"},
              "lon": {"start": 50, "end": 52, "system": "values"}, "time": time_range },
            { "name": "d3", "lat": {"start": 60, "end": 65, "system": "values"},
              "lon": {"start": 50, "end": 52, "system": "values"}, "time": time_range }  ],
        input=[ { "uri":uri, "name":f"tas:v{i}", "domain":f"d{i}" } for i in range(4) ],
        operation=[ { "epa": "edas.subset", "input":f"v{i}"} for i in range(4) ]
    )

    task: Task = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data
    for index,dset in enumerate(dsets):
        dset.to_netcdf( f"/tmp/edas_endpoint_test_result-{index}.nc" )

