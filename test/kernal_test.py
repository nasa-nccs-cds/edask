import dask
from dask.distributed import Client
import xarray as xr
import time, traceback
from xarray.ufuncs import cos
from edask.workflow.internal.xarray import AverageKernel
from edask.workflow.task import Task

if __name__ == '__main__':
    print( "STARTUP" )
    dataset_path = '/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1/*.nc'
    dataset_ncml = '/Users/tpmaxwel/.edas/cache/collections/agg/giss_r1i1p1-tas_Amon_GISS-E2-H_historical_r1i1p1_1.ncml'
    start = time.time()

    dataset = dataset_path
    data_vars = ['tas']

    try:
        client = Client()

        print( "READ " +   dataset )
        dataset: xr.Dataset = xr.open_mfdataset(dataset, autoclose=True, data_vars=data_vars, parallel=True)


        kernel = AverageKernel()
        task = Task( "xarray", "ave", "result", data_vars, { "axes": [ "lat"]})

        workflow = kernel.buildWorkflow( dataset, task )



#        print( "COMPUTE MEAN, Result:" + str( mean.values ) )

        print( " Completed computation in " + str(time.time() - start) + " seconds" )


    except Exception:
        traceback.print_exc()

    finally:
        print( "SHUTDOWN" )