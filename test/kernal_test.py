import dask
from dask.distributed import Client
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xr
import time, traceback
import numpy as np
from edask.workflow.internal.xarray import AverageKernel
from edask.workflow.kernel import Kernel
from edask.workflow.task import Task

if __name__ == '__main__':
    print( "STARTUP" )
    dataset_path = '/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1/*.nc'
    dataset_ncml = '/Users/tpmaxwel/.edas/cache/collections/agg/giss_r1i1p1-tas_Amon_GISS-E2-H_historical_r1i1p1_1.ncml'

    try:
        tstart = time.time()
        client = Client()

        tdefine = time.time()
        print("Defining workflow")
        kernel = AverageKernel()
        task = Task( "xarray", "ave", "result", ['tas'], { "axes": "xyt" } )

        def get_results( task: Task, kernel: Kernel, dataset_path: str ) -> Dict[str,np.ndarray]:
            dataset: xr.Dataset = xr.open_mfdataset(dataset_path, autoclose=True, data_vars=task.inputs, parallel=True)
            workflow = kernel.buildWorkflow(dataset, task)
            results = task.getResults(workflow)
            return { result.name: result.values for result in results }

        tsubmit = time.time()
        result_future = client.submit( get_results, task, kernel, dataset_path )
        print("Submitted computation")
        result_map = result_future.result()
        print( result_map )

        print( "Completed computation in {} seconds, workflow setup time = {}, cluster startup time = {}".format( str(time.time() - tsubmit), str(tsubmit - tdefine), str(tdefine - tstart) ) )

    except Exception:
        traceback.print_exc()

    finally:
        print( "SHUTDOWN" )