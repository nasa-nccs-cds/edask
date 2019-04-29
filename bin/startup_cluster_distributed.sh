#!/usr/bin/env bash

if [ $# -eq 0 ]
  then
    dask-ssh --hostfile $HOME/.edas/conf/hosts --remote-dask-worker distributed.cli.dask_worker --log-directory $HOME/.edas/logs $PKEY_OPTS
  else
    dask-ssh --nprocs $1 --hostfile  $HOME/.edas/conf/hosts --remote-dask-worker distributed.cli.dask_worker --log-directory $HOME/.edas/logs $PKEY_OPTS
fi

# PKEY Example:
# PKEY_OPTS=--ssh-private-key=/home/edaskdev/.ssh/id_edaskdev