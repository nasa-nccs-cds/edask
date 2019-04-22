import logging, time
import numpy.ma as ma
from edas.process.task import Job
from edas.workflow.modules.xarray import *
from dask.distributed import Client, Future, LocalCluster
from edas.config import EdaskEnv
from edas.portal.cluster import EDASCluster
from typing import List, Optional, Tuple, Dict, Any

appConf = { "sources.allowed": "collection,https", "log.metrics": "true"}
EdaskEnv.update(appConf)
cluster = EDASCluster()
print("Initializing Dask-distributed cluster with scheduler address: " + cluster.scheduler_address)
client = Client( cluster.scheduler_address, timeout=60 )
time.sleep(40)
scheduler_info = client.scheduler_info()
workers: Dict = scheduler_info.pop("workers")
print(" @@@@@@@ SCHEDULER INFO: " + str(scheduler_info ))
print(f" N Workers: {len(workers)} " )
