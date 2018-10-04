#!/usr/bin/env bash

datafile='https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml'
ncwa -O -v tas -d lat,0.0,50.0 -d lon,0.0,10.0 -d time,40,50 -a lat,lon -y max ${datafile} /tmp/max1.nc
ncdump /tmp/max1.nc