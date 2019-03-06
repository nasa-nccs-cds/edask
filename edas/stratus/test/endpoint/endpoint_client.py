from stratus_endpoint.handler.base import Task, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
from xarray import Variable
from stratus.handlers.core import StratusCore

if __name__ == "__main__":

    settings = dict( stratus = dict( type="endpoint", module="edas.stratus.endpoint", object="EDASEndpoint" ) )
    stratus = StratusCore( settings )
    client = stratus.getClient()

    requestSpec = dict(
        domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                 "lon": {"start": 40, "end": 42, "system": "values"},
                 "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
        input=[{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}],
        operation=[ { "epa": "edas.subset", "input": "v0"} ]
    )

    task: Task = client.request( requestSpec )
    result: Optional[TaskResult] = task.getResult( block=True )
    if result is None:
        print("NO RESULT!")
    else:
        print("Received result:" )
        print("HEADER: " + str(result.header))
        if result.data is None:
            print( "NO DATA!")
        else:
            print("DATA VARIABLES AND AXES: ")
            for v in result.data.variables.values():
                variable: Variable = v
                print( str( dict( variable.attrs, shape=str(variable.shape) )  ) )
