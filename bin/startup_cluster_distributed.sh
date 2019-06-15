#!/usr/bin/env bash

dask-ssh --hostfile  $HOME/.edas/conf/hosts --remote-dask-worker distributed.cli.dask_worker --log-directory /tmp --memory-limit auto $PKEY_OPTS

# PKEY Example:
# PKEY_OPTS=--ssh-private-key=/home/edaskdev/.ssh/id_edaskdev