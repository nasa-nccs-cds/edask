from edas.process.test import LocalTestManager
import time, xarray as xr
import numpy as np

appConf = {"sources.allowed": "collection,https", "log.metrics": "false"}
mgr = LocalTestManager("PyTest", "world_clim", appConf)
address = mgr.getAddress( "merra2", "tas")

#----------------- READ DATA  -------------------------------------------------------------
t0 = time.time()

ds: xr.Dataset = xr.open_dataset( address, engine='netcdf4' )
ds.compute()

print(" READ --- %s seconds ---" % ( time.time() - t0 ))

#----------------- SERIALIZE  -------------------------------------------------------------
t1 = time.time()

ds_header = ds.to_dict( data=False )
tas: xr.DataArray = ds.data_vars['tas']

tas_headers = tas.to_dict( data=False )
tas_data    = tas.values.tobytes()

print(" SERIALIZE --- %s seconds ---" % ( time.time() - t1 ))

#----------------- DESERIALIZE  -------------------------------------------------------------
t2 = time.time()

tas_headers['data'] = np.frombuffer( tas_data )
new_tas_xarray: xr.DataArray = xr.DataArray.from_dict( tas_headers )

new_ds: xr.Dataset = xr.Dataset.from_dict(ds_header)
new_ds["tas"] = new_tas_xarray

print(" DESERIALIZE --- %s seconds ---" % ( time.time() - t2 ))

#----------------- SAVE RESULTS  -------------------------------------------------------------
t3 = time.time()

new_ds.to_netcdf( "/temp/test_dataset.nc")

print(" SAVE --- %s seconds ---" % ( time.time() - t3 ))
