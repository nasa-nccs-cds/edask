import dask
from dask.distributed import Client
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xr
import time, traceback, logging
import numpy as np
from edask.workflow.internal.xarray import *
from edask.process.operation import WorkflowNode
from edask.workflow.module import edasOpManager
from edask.portal.parsers import WpsCwtParser
from edask.workflow.kernel import Kernel

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.info( "STARTUP" )
    dataset_path = '/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1/*.nc'
    dataset_ncml = '/Users/tpmaxwel/.edas/cache/collections/agg/giss_r1i1p1-tas_Amon_GISS-E2-H_historical_r1i1p1_1.ncml'

    testStr = '[ domain=[ {"name":"d0", "lat":{"start":50,  "end":60, "system":"values" }, "lon":{ "start":30,"end":50, "system":"values" }, "time":{ "start":0,"end":15, "system":"indices" } } ], ' \
              'variable=[{ "collection":"cip_merra2_mon_1980-2015", "name":"tas:v0", "domain":"d0" } ], ' \
              'operation=[{ "name":"xarray.ave", "input":"v0", "domain":"d0","axes":"xy"}, { "name":"xarray.subset", "input":"v0", "domain":"d0"}] ]'

    try:
        tstart = time.time()
        logger.info("Defining workflow")
        dataInputs = WpsCwtParser.parseDatainputs( testStr )
        request: TaskRequest = TaskRequest.new( "requestId", "jobId", dataInputs )
        results: List[EDASDataset] = edasOpManager.buildRequest(request)

        texe = time.time()
        for result in results:
            for variable in result.getVariables():
                result = variable.load()
                logger.info( "\n\n ***** Result {}, shape = {}".format( result.name, str( result.shape ) ) )
                logger.info( result )

        tend = time.time()
        logger.info( "\n Completed computation in {} seconds, workflow setup time = {}, exe time = {}\n".format( str(tend - tstart), str(texe - tstart), str(tend - texe) ) )


    except Exception:
        traceback.print_exc()

    finally:
        logger.info( "SHUTDOWN" )