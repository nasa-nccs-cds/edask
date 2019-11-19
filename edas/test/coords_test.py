import xarray as xa
import numpy as np
import time
import matplotlib
import matplotlib.pyplot as plt


t0 = time.time()

def getSelector(selectionVar: xa.Dataset, selOp: str, taxis: str) -> np.ndarray:
    t0 = time.time()
    lowpassSelector: xa.DataArray = selectionVar.rolling({taxis: 3}, min_periods=3, center=True).mean()
    print( lowpassSelector.shape )
    if selOp == "max":
        xaSelectedMonth: xa.DataArray = lowpassSelector.argmax(taxis, keep_attrs=True)
    elif selOp == "min":
        xaSelectedMonth: xa.DataArray = lowpassSelector.argmin(taxis, keep_attrs=True)
    else:
        raise Exception("Unrecognized operation in getValueForSelectedQuarter: " + selOp)
    selectedMonth: np.ndarray = xaSelectedMonth.to_masked_array()
    print(f" computed Selector, time = {time.time() - t0} sec")
    return selectedMonth

print( "Start")
dset = xa.open_dataset("https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP//reanalysis/MERRA2/mon/atmos/tas.ncml")
tas = dset["tas"]
tas = tas[0:12,:,:].compute()
t1 = time.time()

selectedMonth: np.ndarray = getSelector( tas, "max", "time" )

t2 = time.time()

tcoord = tas.time
tlen = tcoord.size
tdata: np.ndarray = np.arange(tlen).reshape(tlen, 1, 1)
mask = (np.abs((tdata-selectedMonth)) < 2)

tave = tas.where( mask ).mean( dim="time").compute()

t3 = time.time()

print( tave )
print( f" {t1-t0} {t2-t1} {t3-t2} " )

result = xa.DataArray( mask[0], dims = tas.dims[1:] )
result.plot()
plt.show()