#!/usr/bin/env bash

dask-scheduler &

dask-worker 127.0.0.1:8786 &
dask-worker 127.0.0.1:8786 &
dask-worker 127.0.0.1:8786 &
dask-worker 127.0.0.1:8786 &

