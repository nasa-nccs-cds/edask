from pc2.module.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import xarray as xa
from stratus.app.core import StratusCore
from stratus.app.operations import ClientOpSet

settings = dict( stratus=dict(type="zeromq", request_port="4556", response_port="4557" ),
                 edas=dict( type="endpoint", module="edas.stratus.endpoint", object="EDASEndpoint" ) )
core = StratusCore(settings)
app = core.getApplication()

request = {    "edas:domain": [ { "name": "d0", "time": {"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"} } ],
                "edas:input":  [ { "uri": "collection:merra2", "name": "tas:v1", "domain": "d1" } ],
                "operation":   [ { "name": "edas:ave", "input": "v1", "axis": "yt", "result": "v1ave" },
                                { "name": "edas:diff", "input": ["v1", "v1ave"] } ] }

clientOpsets: Dict[str, ClientOpSet] = app.geClientOpsets(request)
distributed_opSets = app.distributeOps(clientOpsets)