import dask
from dask.distributed import Client
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xa
import time, traceback, logging
import numpy as np
from edas.workflow.modules.xarray import *
from edas.util.logging import EDASLogger
from edas.workflow.module import edasOpManager
from edas.portal.parsers import WpsCwtParser

if __name__ == '__main__':
    logger = EDASLogger.getLogger()
    logger.info( "STARTUP" )
    dataset_path = '/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1/*.nc'
    dataset_ncml = '/Users/tpmaxwel/.edas/cache/collection/agg/giss_r1i1p1-tas_Amon_GISS-E2-H_historical_r1i1p1_1.ncml'

    testStr = '[ domain=[ {"name":"d0",   \n   "lat":{"start":0.0,  "end":20.0, "system":"values" }, "lon":{ "start":0.0,"end":20.0, "system":"values" }, "time":{ "start":0,"end":20, "system":"indices" } } ], ' \
              'variable=[{ "collection":"cip_merra2_mon_1980-2015", "name":"tas:v0", "domain":"d0" } ], ' \
              'operation=[{ "name":"xarray.ave", "input":"v0", "domain":"d0","axes":"xy"}] ]'

    try:
        tstart = time.time()
        client = Client()

        tdefine = time.time()
        logger.info("Defining workflow")

        def get_results( ) -> List[xa.Dataset]:
            dataInputs = WpsCwtParser.parseDatainputs( testStr )
            request: TaskRequest = TaskRequest.new( "requestId", "jobId", dataInputs )
            return edasOpManager.buildRequest( request )

        tsubmit = time.time()
        result_future = client.submit( get_results )
        logger.info("Submitted computation")
        results: List[EDASDataset] = result_future.result()

        logger.info( "\n Completed computation in {} seconds, workflow setup time = {}, cluster startup time = {}\n".format( str(time.time() - tsubmit), str(tsubmit - tdefine), str(tdefine - tstart) ) )

        for result in results:
            for variable in result.inputs:
                logger.info( "\n Result: ")
                logger.info( variable )


    except Exception:
        traceback.print_exc()

    finally:
        logger.info( "SHUTDOWN" )
