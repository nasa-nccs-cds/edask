#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ $# -eq 0 ]
  then
    dask-ssh --hostfile $DIR/../resources/hosts $PKEY_OPTS
  else
    dask-ssh --nprocs $1 --hostfile $DIR/../resources/hosts $PKEY_OPTS
fi