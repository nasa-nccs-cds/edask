#!/usr/bin/env bash

datafile=${HOME}/Dropbox/Tom/Data/MERRA/TEST/tas.ncml.nc
ncks -O -v tas -d lat,50.0,55.0 -d lon,40.0,42.0 -d time,10,15  ${datafile} /tmp/subset1.nc
ncdump /tmp/subset1.nc