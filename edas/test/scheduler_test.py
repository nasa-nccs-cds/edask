import logging, time
import numpy.ma as ma
from edas.process.task import Job
from edas.workflow.modules.xarray import *
from edas.workflow.module import edasOpManager
from edas.process.manager import ExecHandler
from dask.distributed import Client, Future, LocalCluster
from edas.config import EdaskEnv
from edas.portal.cluster import EDASCluster
from typing import List, Optional, Tuple, Dict, Any

collection = "cip_eraint_mth"
variable = "ta"
time_range = [ "1981-01-01", "2011-01-01"]

domains = [{"name": "d0", "time": {"start": time_range[0], "end": time_range[1], "crs": "timestamps"}}]
variables = [{"uri": f"collection://{collection}:", "name": f"{variable}:v0", "domain": "d0"}]
operations = [{"name": "xarray.ave", "input": "v0", "axes": "t"}]

appConf = { "sources.allowed": "collection,https", "log.metrics": "true"}
EdaskEnv.update(appConf)
cluster = EDASCluster()
print("Initializing Dask-distributed cluster with scheduler address: " + cluster.scheduler_address)
client = Client( cluster.scheduler_address, timeout=60 )
time.sleep(30)
scheduler_info = client.scheduler_info()
workers: Dict = scheduler_info.pop("workers")
print(" @@@@@@@ SCHEDULER INFO: " + str(scheduler_info ))
print(f" N Workers: {len(workers)} " )
start_time = time.time()
job = Job.init( "Test", "SCHEDULER_TEST", "jobId", domains, variables, operations )
print("Running workflow for requestId " +  job.requestId )
result = edasOpManager.buildTask( job )
print("Completed workflow in time " + str(time.time() - start_time))
resultHandler = ExecHandler("local", job, workers=job.workers)
resultHandler.processResult(result)
