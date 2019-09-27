from pc2base.module.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import os, xarray as xa
from stratus.app.core import StratusCore
USE_OPENDAP = False

if __name__ == "__main__":
#    certificate_path = "/att/nobackup/tpmaxwel/.stratus/zmq/"
    certificate_path = "/Users/tpmaxwel/.stratus/zmq"
    settings = dict( stratus = dict( type="zeromq", client_address = "127.0.0.1", request_port = "4556", response_port = "4557", certificate_path = certificate_path ) )
    stratus = StratusCore( settings )
    client = stratus.getClient()
    time_range = {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}
    uri =  mgr.getAddress("merra2", "tas") if USE_OPENDAP else "collection://cip_merra2_mth"

    requestSpec = dict(
        domain=[ dict(name="d0", time=time_range)  ],
        input=[ dict( uri=uri, name="tas:v0", domain="d0" ) ],
        operation=[ dict( name="edas:ave", axis="xy", input="v0" )  ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data
    for index,dset in enumerate(dsets):
        fileName =  f"/tmp/edas_endpoint_test_result-{index}.nc"
        print( f"Got result[{index}]: Saving to file {fileName} " )
        dset.to_netcdf( fileName )