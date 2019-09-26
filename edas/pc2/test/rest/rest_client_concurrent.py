from pc2.module.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.process.test import TestDataManager as mgr
import xarray as xa, time
from stratus.app.core import StratusCore

if __name__ == "__main__":

    settings = dict( stratus=dict(type="rest", host="127.0.0.1", port="5000" ) )
    stratus = StratusCore(settings)
    client = stratus.getClient()
    time_range = {"start": "1980-01-01", "end": "2001-12-31", "crs": "timestamps"}
    uri = mgr.getAddress("cip_merra2_mth", "tas")


    t1 = time.time()

    requestSpec1 = dict(
        domain=[ dict(name="d0", lat=dict(start=0, end=80, system="values"), time=time_range) ],
        input=[dict(uri=uri, name=f"tas:v0", domain="d0") ],
        operation=[dict(name="edas:ave", axis="xy", input="v0") ],
        rid = "r1",
        cid = "c1"
    )
    task1: TaskHandle = client.request(requestSpec1)
    result1: Optional[TaskResult] = task1.getResult(block=True)
    dsets1: List[xa.Dataset] = result1.data
    fileName = f"/tmp/edas_endpoint_test1_result.nc"
    print( f"Got result: Saving to file {fileName} " )

    print(" Completed request1 in " + str(time.time() - t1) + " seconds")



    t2 = time.time()

    requestSpec2 = dict(
        domain=[ dict(name="d0", lat=dict(start=-80, end=0, system="values"), time=time_range) ],
        input=[dict(uri=uri, name=f"tas:v0", domain="d0") ],
        operation=[dict(name="edas:ave", axis="xy", input="v0") ],
        rid = "r1",
        cid = "c1"
    )
    task2: TaskHandle = client.request(requestSpec2)
    result2: Optional[TaskResult] = task2.getResult(block=True)
    dsets2: List[xa.Dataset] = result2.data
    fileName = f"/tmp/edas_endpoint_test2_result.nc"
    print( f"Got result: Saving to file {fileName} " )

    print(" Completed request2 in " + str(time.time() - t2) + " seconds")

