from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import xarray as xa
from stratus.app.core import StratusCore

if __name__ == "__main__":

    settings = dict( stratus = dict( type="zeromq", client_address = "foyer101", request_port = "4556", response_port = "4557" ) )
    stratus = StratusCore( settings )
    client = stratus.getClient()

    time_range = {"start": "2000-01-01T00Z", "end": "2001-01-01T00Z", "system": "timestamps"}
    uri = "collection://merra2_inst1_2d_asm_Nx"

    requestSpec = dict(
        domain=[ dict(name="d0", time=time_range) ],
        input=[ dict( uri=uri, name="T2M:temp", domain="d0" ), dict(uri=uri, name="QV2M:moist", domain="d0") ],
        operation=[ dict( name="edas:worldClim", input="temp,moist" ) ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data

