#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='edas',
      version='1.0',
      description='EDASK: Earth Data Analytic Services using the dasK / xarray toolkit',
      author='Thomas Maxwell',
      author_email='thomas.maxwell@nasa.gov',
      url='https://github.com/nasa-nccs-cds/edas.git',
      scripts=['bin/startup_scheduler', 'bin/startup_cluster_local.sh', 'bin/startup_cluster_distributed.sh'],
      packages=find_packages(exclude="sandbox")
)
