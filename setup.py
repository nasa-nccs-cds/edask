#!/usr/bin/env python

from setuptools import setup, find_packages
with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = set()
with open( "requirements.txt" ) as f:
  for dep in f.read().split('\n'):
      if dep.strip() != '' and not dep.startswith('-e'):
          install_requires.add( dep )

setup(name='edas',
      version='1.0',
      description='EDAS: Earth Data Analytic Services using the dasK / xarray toolkit',
      author='Thomas Maxwell',
      author_email='thomas.maxwell@nasa.gov',
      url='https://github.com/nasa-nccs-cds/edask.git',
      long_description=long_description,
      long_description_content_type="text/markdown",
      scripts=['bin/startup_scheduler', 'bin/startup_cluster.sh', 'bin/startup_cluster_dask.sh'],
      packages=find_packages(),
      install_requires=list(install_requires),
      classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
