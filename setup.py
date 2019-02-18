#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='edas',
      version='1.0',
      description='EDAS: Earth Data Analytic Services using the dasK / xarray toolkit',
      author='Thomas Maxwell',
      author_email='thomas.maxwell@nasa.gov',
      url='https://github.com/nasa-nccs-cds/edask.git',
      scripts=['bin/startup_scheduler', 'bin/startup_cluster.sh', 'bin/startup_cluster_dask.sh'],
      packages=find_packages(),
)
