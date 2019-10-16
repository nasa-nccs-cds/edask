import logging, time
import numpy.ma as ma
from edas.process.task import Job
from edas.workflow.modules.xarray import *
from edas.workflow.module import edasOpManager
from edas.process.manager import ExecHandler
from dask.distributed import Client, Future, LocalCluster
from edas.config import EdasEnv
from edas.portal.cluster import EDASCluster
from typing import List, Optional, Tuple, Dict, Any

collection = "merra_daily"
variable = "t"

domains = [{"name": "d0"}]
variables = [{"uri": f"collection://{collection}:", "name": f"{variable}:v0", "domain": "d0"}]
operations = [{"name": "xarray.ave", "input": "v0", "axes": "t"}]
local = True
scheduler = "127.0.0.1:8786"

if __name__ == '__main__':
    print(f"Running test")
    appConf = { "sources.allowed": "collection,https", "log.metrics": "true"}
    EdasEnv.update( appConf )

    if local:
        print( f"Initializing Local Dask cluster" )
        client = Client()
    else:
        if scheduler is None:
            cluster = EDASCluster()
            print("Initializing Dask-distributed cluster with scheduler address: " + cluster.scheduler_address)
            client = Client( cluster.scheduler_address, timeout=64 )
            time.sleep(20)
        else:
            print("Initializing client with existing scheduler at: " + scheduler )
            client = Client(scheduler)

    scheduler_info = client.scheduler_info()
    workers: Dict = scheduler_info.pop("workers")
    print(" @@@@@@@ SCHEDULER INFO: " + str(scheduler_info ))
    print(f" N Workers: {len(workers)} " )

    start_time1 = time.time()
    job1 = Job.init( "Test", "SCHEDULER_TEST", "jobId", domains, variables, operations, [] )
    print("Running workflow for requestId " +  job1.requestId )
    result1 = edasOpManager.buildTask( job1 )
    print("Completed first workflow in time " + str(time.time() - start_time1))

    start_time2 = time.time()
    job2 = Job.init( "Test", "SCHEDULER_TEST", "jobId", domains, variables, operations, []  )
    print("Running workflow for requestId " +  job2.requestId )
    result2 = edasOpManager.buildTask( job2 )
    print("Completed second workflow in time " + str(time.time() - start_time2))

    resultHandler2 = ExecHandler("local", job2, workers=job2.workers)
    resultHandler2.processResult(result2)
