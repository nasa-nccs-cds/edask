#!/usr/bin/env python
import os
from setuptools import setup, find_packages
with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = set()
with open( "requirements.txt" ) as f:
  for dep in f.read().split('\n'):
      if dep.strip() != '' and not dep.startswith('-e'):
          install_requires.add( dep )

from shutil import copyfile
HERE = os.path.dirname(__file__)
CONFIG_FILE = os.path.join( HERE, "resources", 'app.conf.template' )
HOSTS = os.path.join( HERE, "resources", 'hosts' )
CONFIG_DIR = os.environ.get('EDAS_CONFIG_DIR', os.path.expanduser("~/.edas/conf" ) )
try: os.makedirs( CONFIG_DIR )
except: pass
INSTALLED_CONFIG_FILE=os.path.join( CONFIG_DIR, 'app.conf' )
INSTALLED_HOST_FILE=os.path.join( CONFIG_DIR, 'hosts' )
if not os.path.isfile(INSTALLED_CONFIG_FILE):
      copyfile( CONFIG_FILE, INSTALLED_CONFIG_FILE )
      print( f"Installing edas config file 'app.conf'' into directory '{CONFIG_DIR}'")
if not os.path.isfile(INSTALLED_HOST_FILE):
      copyfile( HOSTS, INSTALLED_HOST_FILE )
      print( f"Installing edas hosts file 'hosts' into directory '{CONFIG_DIR}'" )


setup(name='edas',
      version='1.0',
      zip_safe=False,
      description='EDAS: Earth Data Analytic Services using the dasK / xarray toolkit',
      author='Thomas Maxwell',
      author_email='thomas.maxwell@nasa.gov',
      url='https://github.com/nasa-nccs-cds/edas.git',
      scripts=['bin/startup_scheduler', 'bin/startup_cluster_local.sh', 'bin/startup_cluster_distributed.sh'],
      packages=find_packages(exclude="sandbox")
)
