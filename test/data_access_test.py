import xarray as xa
import numpy as np
varName = "ts"
# data_address = 'https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/Reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos/' + varName + '.ncml'
data_address = 'https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/20CRv2c/mon/atmos/' + varName + '.ncml'
dset = xa.open_dataset( data_address, autoclose=True  )
ts: xa.Variable = dset.variables[varName]
data: np.ndarray = ts.values.flatten()
print( data[0:500] )


