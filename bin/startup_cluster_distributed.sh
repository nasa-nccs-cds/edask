#!/usr/bin/env bash
LOG_DIR=${EDASK_CACHE_DIR:=/tmp}
dask-ssh --hostfile  $HOME/.edas/conf/hosts --remote-dask-worker distributed.cli.dask_worker --log-directory $LOG_DIR $PKEY_OPTS

# PKEY Example:
# PKEY_OPTS=--ssh-private-key=/home/edaskdev/.ssh/id_edaskdev