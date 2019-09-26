from pc2.module.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import xarray as xa
from stratus.app.core import StratusCore

settings = dict(stratus=dict(type="zeromq", client_address="127.0.0.1", request_port="4556", response_port="4557"))
stratus = StratusCore(settings)
client = stratus.getClient()
base_year = 1990
nYears = 1

variables_1h = [{"uri": "collection://merra2_inst1_2d_asm_Nx", "name": "T2M:temp", "domain": "d0"},
                {"uri": "collection://merra2_inst1_2d_asm_Nx", "name": "QV2M:moist", "domain": "d0"}]

variables_6h = [{"uri": "collection://cip_merra2_6hr", "name": "tas:temp", "domain": "d0"},
                {"uri": "collection://cip_merra2_6hr", "name": "pr:moist", "domain": "d0"}]

operations = [{"name": "edas.worldClim", "input": "temp,moist"}]
domains = [{"name": "d0", "time": {"start": f'{base_year}-01-01T00Z', "end": f'{base_year+nYears-1}-12-31T23Z', "system": "timestamps"}}]

requestSpec = dict( domain=domains, input = variables_1h, operation = operations )
task: TaskHandle = client.request(requestSpec)
result: Optional[TaskResult] = task.getResult(block=True)
for dataset in result.data:
    print( f"Got result, vars = {dataset.variables.keys()}")
