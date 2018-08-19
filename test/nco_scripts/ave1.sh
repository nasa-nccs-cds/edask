#!/usr/bin/env bash

datafile=${HOME}/Dropbox/Tom/Data/MERRA/TEST/tas.ncml.nc
ncwa -O -v tas -d lat,30.0,80.0 -d lon,0.0,100.0 -d time,0,40 -a lat,lon  ${datafile} /tmp/ave1.nc
ncdump /tmp/ave1.nc