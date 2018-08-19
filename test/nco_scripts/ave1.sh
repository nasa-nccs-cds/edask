#!/usr/bin/env bash

datafile=${HOME}/Dropbox/Tom/Data/MERRA/TEST/tas.ncml.nc
ncwa -O -v tas -d lat,0.0,50.0 -d lon,0.0,10.0 -d time,40,50 -a lat,lon  ${datafile} /tmp/ave1.nc
ncdump /tmp/ave1.nc