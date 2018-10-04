#!/usr/bin/env bash

datafile1='https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml'
ncks -O -v tas -d lat,50.0,55.0 -d lon,40.0,45.0 -d time,10,11  ${datafile1} /tmp/var1.nc

datafile2='https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml'
ncks -O -v tas -d lat,50.0,55.0 -d lon,40.0,45.0 -d time,10,11  ${datafile2} /tmp/var2.nc

ncbo -O -v tas /tmp/var1.nc /tmp/var2.nc /tmp/out.nc

ncdump /tmp/out.nc
