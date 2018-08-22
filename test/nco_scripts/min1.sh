#!/usr/bin/env bash

datafile='https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml'
ncwa -O -v tas -d lat,50,100 -d lon,30,120 -d time,30,50 -a lat,lon -y min ${datafile} /tmp/out.nc
# ncks -O -v tas -d lat,50,100 -d lon,30,120 -d time,30,50  ${datafile} /tmp/out.nc
ncdump /tmp/out.nc