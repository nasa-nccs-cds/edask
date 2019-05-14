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

collection = "cip_eraint_mth"
variable = "ta"
time_range = [ "1981-01-01", "2011-01-01"]
local = True
# scheduler = "edaskwndev01:8786"
scheduler = "foyer101:8786"

domains = [{"name": "d0", "time": {"start": time_range[0], "end": time_range[1], "crs": "timestamps"}}]
variables = [{"uri": f"collection://{collection}:", "name": f"{variable}:v0", "domain": "d0"}]
operations = [{"name": "edas.ave", "input": "v0", "axes": "t"}]

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
            client = Client( cluster.scheduler_address, timeout=60 )
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
