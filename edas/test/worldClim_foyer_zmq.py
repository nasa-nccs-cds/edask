from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import os, xarray as xa
from stratus.app.core import StratusCore
OUTPUT_DIR = os.path.expanduser("~/.worldClim/results")
os.makedirs(OUTPUT_DIR,exist_ok=True)

if __name__ == "__main__":

    settings = dict( stratus = dict( type="zeromq", client_address = "foyer101", request_port = "4556", response_port = "4557" ) )
    stratus = StratusCore( settings )
    client = stratus.getClient()

    time_range = {"start": "1990-01-01T00Z", "end": "1991-01-01T00Z", "system": "timestamps"}

#    uri = "collection://merra2_inst1_2d_asm_Nx"
#    vars = dict(temp="T2M", moist="QV2M")

#    collection = "cip_merra2_6hr"
#    vars = dict( temp="tas", moist="pr" )

    collection = "cip_merra2_mth"
    vars = dict( minTemp="tasmin", maxTemp="tasmax", moist="pr" )

    requestSpec = dict(
        domain=[ dict(name="d0", time=time_range) ],
        input=[ dict( uri=f"collection://{collection}", name=f"{vars['minTemp']}:tempMin",   domain="d0" ),
                dict( uri=f"collection://{collection}", name=f"{vars['maxTemp']}:tempMax",   domain="d0"),
                dict( uri=f"collection://{collection}", name=f"{vars['moist']}:moist",       domain="d0" )   ],
        operation=[ dict( name="edas:worldClim", input="minTemp,maxTemp,moist" ) ]
    )

    task: TaskHandle = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    resultFile = f"{OUTPUT_DIR}/cip_merra2_mth_worldclim_1990.nc"
    print( f"Saving result to {resultFile}" )
    result.getDataset().to_netcdf( resultFile )

