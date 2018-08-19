#!/usr/bin/env bash

datafile=${HOME}/Dropbox/Tom/Data/MERRA/TEST/tas.ncml.nc
ncwa -O -v tas -d lat,150,155 -d lon,40,43 -d time,40,50 -a lat,lon  ${datafile} /tmp/ave1.nc
ncdump /tmp/ave1.nc