#!/usr/bin/env bash

datafile='https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml'
ncks -O -v tas -d lat,50.0,55.0 -d lon,40.0,42.0 -d time,10,15  ${datafile} /tmp/subset1-dap.nc
ncdump /tmp/subset1-dap.nc