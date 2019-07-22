from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import xarray as xa
from stratus.app.core import StratusCore

if __name__ == "__main__":

    settings = dict( stratus = dict( type="zeromq", client_address = "foyer101", request_port = "4556", response_port = "4557" ) )
    stratus = StratusCore( settings )
    client = stratus.getClient()

    time_range = {"start": "1980-01-01T00Z", "end": "1981-01-01T00Z", "system": "timestamps"}

#    uri = "collection://merra2_inst1_2d_asm_Nx"
#    vars = dict(temp="T2M", moist="QV2M")

#    collection = "cip_merra2_6hr"
#    vars = dict( temp="tas", moist="pr" )

    collection = "cip_merra2_mth"
    vars = dict( tempMin="tasmin", tempMax="tasmax", moist="pr" )

    requestSpec = dict(
        domain=[ dict(name="d0", time=time_range) ],
        input=[ dict( uri=f"collection://{collection}", name=f"{vars['temp']}:temp",   domain="d0" ),
                dict( uri=f"collection://{collection}", name=f"{vars['moist']}:moist", domain="d0" )   ],
        operation=[ dict( name="edas:worldClim", input="temp,moist" ) ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    dsets: List[xa.Dataset] = result.data

