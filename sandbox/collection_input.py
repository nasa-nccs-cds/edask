import xarray as xa
import time, traceback
from dask.distributed import Client
from edas.collection.agg import Collection

print ( "STARTUP" )
client = None
start = time.time()
collection = "cip_merra2_mth"
varName = 'KE'

try:
    client = Client('cldradn101:8786')

    print ( "READ " + collection )

    collection = Collection.new( collection )
    ds = xa.open_mfdataset( collection.pathList(varName), autoclose=True, data_vars=['KE'], parallel=True)

    print ( "COMPUTE MEAN, Result:" )

    lat_bnds, lon_bnds = [40, 43], [-96, -89]              # use CONUS bounds
    ds.sel(lat=slice(*lat_bnds), lon=slice(*lon_bnds))

    print ( ds.KE.mean().values )

    print ( " Completed computation in " + str(time.time() - start) + " seconds" )

except Exception:
    traceback.print_exc()

finally:
    print ( "SHUTDOWN" )
    if client: client.close()
