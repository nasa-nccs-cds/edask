from dask.distributed import Client
import xarray as xr
import time

if __name__ == "__main__":

    client = Client(n_workers=2, threads_per_worker=2, memory_limit='1GB')

    time.sleep(10)

    ds = xr.tutorial.open_dataset('air_temperature', chunks={'lat': 25, 'lon': 25, 'time': -1})

    da = ds['air']

    da2 = da.groupby('time.month').mean('time')
    da3 = da - da2

    computed_da = da3.load()

    print( computed_da )