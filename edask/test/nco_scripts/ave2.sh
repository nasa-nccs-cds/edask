#!/usr/bin/env bash
rm /tmp/*.nc
datafile='https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml'
ncks -O -v tas -d lat,0.0,50.0 -d lon,0.0,100.0 -d time,30,50  ${datafile} /tmp/subset-data.nc
ncap2 -O -S cosine_weights.nco /tmp/subset-data.nc /tmp/subset-data-plus-wts.nc
ncwa -O -w gw -a lat,lon /tmp/subset-data-plus-wts.nc  /tmp/out.nc
ncdump /tmp/out.nc