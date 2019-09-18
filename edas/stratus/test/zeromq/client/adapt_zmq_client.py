from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import xarray as xa
from stratus.app.core import StratusCore

if __name__ == "__main__":
    certificate_path = "/att/nobackup/tpmaxwel/.stratus/zmq/"
    settings = dict( stratus = dict( type="zeromq", client_address = "foyer101", request_port = "4556", response_port = "4557", certificate_path = certificate_path  ) )
    stratus = StratusCore( settings )
    client = stratus.getClient()
    time_range = {"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"}
    uri =  "collection://cip_merra2_mth"

    requestSpec = dict(
        domain=[ dict(name="d0", lat=dict(start=-80, end=80, system="values"), lon=dict(start=0, end=100, system="values"), time=time_range) ],
        input=[ dict( uri=uri, name="tas:v0", domain="d0" ) ],
        operation=[ dict( name="edas:ave", axis="xy", input="v0" ) ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data
    for index,dset in enumerate(dsets):
        print( f"Got dataset {index}: {dset}")
