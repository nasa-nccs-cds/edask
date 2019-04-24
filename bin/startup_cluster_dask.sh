#!/usr/bin/env bash

if [ $# -eq 0 ]
  then
    dask-ssh --hostfile $HOME/.edas/conf/hosts $PKEY_OPTS
  else
    dask-ssh --nprocs $1 --hostfile  $HOME/.edas/conf/hosts $PKEY_OPTS
fi
