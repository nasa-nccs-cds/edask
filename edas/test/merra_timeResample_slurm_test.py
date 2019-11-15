from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import os, xarray as xr
from stratus.app.core import StratusCore
from edas.config import EdasEnv
import traceback
OUTPUT_DIR = os.path.expanduser("/att/pubrepo/ILAB/data/cache/results")
os.makedirs(OUTPUT_DIR,exist_ok=True)
appConf = {'scheduler.address': 'slurm:default' }
EdasEnv.update(appConf)

settings = dict(stratus=dict(type="endpoint", module="edas.stratus.endpoint", object="EDASEndpoint"))
stratus = StratusCore(settings)
client = stratus.getClient()

collection = "cip_merra2_6hr"
vnames = dict( temp='tas', precip='pr', humid='hur', clouds='clt' )

singleYear = True
base_year = 2014
nYears = 1

variables = [ dict( uri=f"collection://{collection}", name=f"{vnames[vtype]}:{vtype}", domain="d0") for vtype in ['temp','precip','humid','clouds'] ]

domains0 = [ { "name": "d0" } ]
domains1 = [ {"name": "d0", "time": {"start": f'{base_year}-01-01T00Z', "end": f'{base_year+nYears-1}-12-31T23Z', "system": "timestamps"} } ]
domains = domains1 if singleYear else domains0

def compute( requestSpec: Dict, rid: str ):
    try:
        task: TaskHandle = client.request(requestSpec)
        result: Optional[TaskResult] = task.getResult(block=True)
        result.getDataset().to_netcdf( f"{OUTPUT_DIR}/{rid}.nc" )
    except Exception as err:
        traceback.print_exc()

print( "Compute temperature products ------------------------------------------------------------------------------------------------------------------------" )

operations = [ { "name": "edas.timeResample", "input": "temp", "freq": "1D", "op": "ave,max,min" } ]
requestSpec = dict( domain=domains, input = variables, operation = operations )
compute( requestSpec, f"merra2-{base_year}-temp-dailyMinMaxAve" )


print( "Compute precipitation products ----------------------------------------------------------------------------------------------------------------------" )

operations = [ { "name": "edas.timeResample", "input": "precip", "freq": "1D", "op": "ave,max,min" } ]
requestSpec = dict( domain=domains, input = variables, operation = operations )
compute( requestSpec, f"merra2-{base_year}-precip-dailyMinMaxAve" )


print( "Compute cloud cover products ------------------------------------------------------------------------------------------------------------------------" )

operations = [ { "name": "edas.timeResample", "input": "clouds", "freq": "1D", "op": "ave,max,min" } ]
requestSpec = dict( domain=domains, input = variables, operation = operations )
compute( requestSpec, f"merra2-{base_year}-clouds-dailyMinMaxAve" )

print( "Compute relative humidity products ------------------------------------------------------------------------------------------------------------------" )

operations = [ { "name": "edas.timeResample", "input": "humid", "freq": "1D", "op": "ave,max,min" } ]
requestSpec = dict( domain=domains, input = variables, operation = operations )
compute( requestSpec, f"merra2-{base_year}-humid-dailyMinMaxAve" )


print( "Completed processing ------------------------------------------------------------------------------------------------------------------------" )




