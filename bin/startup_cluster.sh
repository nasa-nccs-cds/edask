#!/usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

dask-ssh --nprocs $1 --hostfile $DIR/../resources/hosts
