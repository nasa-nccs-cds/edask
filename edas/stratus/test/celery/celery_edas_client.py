from pc2base.module.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import os, xarray as xa
from stratus.app.core import StratusCore
HERE: str = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE: str = os.path.join( HERE, "edas_endpoint_settings.ini" )

if __name__ == "__main__":

    stratus = StratusCore( SETTINGS_FILE )
    client = stratus.getClient()
    time_range = {"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"}
    uri =  mgr.getAddress("merra2", "tas")
    domains = [ f"d{i}" for i in range(4)]

    requestSpec = dict(

        domain=[
            dict(name="d0", lat=dict(start=20, end=30, system="values"), lon=dict(start=0, end=10, system="values"), time=time_range),
            dict(name="d1", lat=dict(start=30, end=40, system="values"), lon=dict(start=10, end=20, system="values"), time=time_range),
            dict(name="d2", lat=dict(start=50, end=60, system="values"), lon=dict(start=20, end=30, system="values"), time=time_range),
            dict(name="d3", lat=dict(start=60, end=70, system="values"), lon=dict(start=30, end=40, system="values"), time=time_range), ],

        input=[ dict( uri=uri, name=f"tas:v{i}", domain=f"d{i}" ) for i in range(4) ],

        operation=[ dict( name="edas:ave", axis="xy", input=f"v{i}" ) for i in range(4) ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data
    for index,dset in enumerate(dsets):
        fileName =  f"/tmp/edas_endpoint_test_result-{index}.nc"
        print( f"Got result[{index}]: Saving to file {fileName} " )
        dset.to_netcdf( fileName )

