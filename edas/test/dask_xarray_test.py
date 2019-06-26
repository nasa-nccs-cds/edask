from dask.distributed import Client
import xarray as xr
import time

if __name__ == "__main__":

    files = [ f"/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/DAILY/2005/JAN/MERRA300.prod.assim.inst3_3d_asm_Cp.200501{i}.SUB.nc" for i in range(10,31) ]

    client = Client(n_workers=2, threads_per_worker=2, memory_limit='1GB')

    time.sleep(10)

    ds: xr.Dataset = xr.open_mfdataset(files, engine='netcdf4', data_vars=['t'])

 #   ds = xr.tutorial.open_dataset('air_temperature', chunks={'lat': 25, 'lon': 25, 'time': -1})

    da: xr.DataArray = ds['t']

    da2: xr.DataArray = da.groupby('time.month').mean('time')

    da_monthly = da2.load()

    print( da_monthly )
