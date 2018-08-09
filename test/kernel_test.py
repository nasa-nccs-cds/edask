import dask
from dask.distributed import Client
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xr
import time, traceback
import numpy as np
from edask.workflow.internal.xarray import *
from edask.workflow.task import Task
from edask.workflow.module import edasOpManager

if __name__ == '__main__':
    print( "STARTUP" )
    dataset_path = '/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1/*.nc'
    dataset_ncml = '/Users/tpmaxwel/.edas/cache/collections/agg/giss_r1i1p1-tas_Amon_GISS-E2-H_historical_r1i1p1_1.ncml'

    try:
        tstart = time.time()
        client = Client()

        tdefine = time.time()
        print("Defining workflow")

        aveTask = Task( "xarray", "ave", "result", ['tas'], { "axes": "xyt" } )
        inputTask = Task(  "xarray", "input", "result", ['tas'], { "file": '/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1/*.nc' } )

        def get_results( ) -> List[xr.DataArray]:
            from edask.workflow.kernel import Kernel
            inputKernel: Kernel =  edasOpManager.getKernel(inputTask)
            workflow = inputKernel.buildWorkflow( inputTask, None )
            aveKernel: Kernel =  edasOpManager.getKernel(aveTask)
            workflow = aveKernel.buildWorkflow( aveTask, workflow )
            return aveTask.getResults(workflow)

        tsubmit = time.time()
        result_future = client.submit( get_results )
        print("Submitted computation")
        results = result_future.result()
        print( results )

        print( "Completed computation in {} seconds, workflow setup time = {}, cluster startup time = {}".format( str(time.time() - tsubmit), str(tsubmit - tdefine), str(tdefine - tstart) ) )

    except Exception:
        traceback.print_exc()

    finally:
        print( "SHUTDOWN" )