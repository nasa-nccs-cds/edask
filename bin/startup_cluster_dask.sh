#!/usr/bin/env bash

if [ $# -eq 0 ]
  then
    dask-ssh --hostfile $HOME/.edas/conf/hosts-2 --remote-dask-worker distributed.cli.dask_worker --log-directory $HOME/.edas/logs $PKEY_OPTS
  else
    dask-ssh --nprocs $1 --hostfile  $HOME/.edas/conf/hosts-2 --remote-dask-worker distributed.cli.dask_worker --log-directory $HOME/.edas/logs $PKEY_OPTS
fi



# mkdir -p /home/edaskdev/.edas/logs && /dass/dassnsd/data01/sys/edaskdev/anaconda3/envs/edask/bin/python -m distributed.cli.dask_worker 10.71.13.11:8786 --nthreads 0 --host edaskwndev13 &> /home/edaskdev/.edas/logs/dask_scheduler_edaskwndev13.lo
