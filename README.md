### EDASK

##### Earth Data Analytic Services using the dasK / xarray toolkit.

EDASK is a high performance big data analytics and machine learning framework. This framework enables scientists to execute data processing workflows combining common analysis and forecast operations close to the massive data stores at NASA. The data is accessed in standard (NetCDF, HDF, etc.) formats in a POSIX file system and processed using vetted tools of earth data science, e.g. ESMF, CDAT, NCO, Keras, Tensorflow, etc.  EDAS facilitates the construction of high performance parallel workflows by combining canonical analytic operations to enable processing of huge datasets within limited memory spaces with interactive response times. EDAS services are accessed via a WPS API being developed in collaboration with the ESGF Compute Working Team to support server-side analytics for ESGF. Client packages in Python, Java/Scala, or JavaScript contain everything needed to build and submit EDAS requests.   

#### Installation

To install:
```
    > conda create -n edask -c conda-forge -c cdat python=3.6 cdms2 cdutil cdtime vcs
    > conda activate edask
    > conda install xarray matplotlib scipy bottleneck paramiko netCDF4 defusedxml python-graphviz bokeh pyparsing pillow scikit-learn tensorflow keras zeromq pyzmq pytest
    > conda install -c conda-forge libnetcdf nco eofs dask distributed
    > pip install pydap
    
    > git clone https://github.com/nasa-nccs-cds/edask.git
    > cd edask
    > python setup.py install

```

#### Configuration

The edask configuration files are found in the edask/resources direcory.
To set up a cluster, copy the edask/resources/hosts.template file to edask/resources/hosts and edit to enter the names of the workers in your cluster.
To setup EDASK server configuration, copy the edask/resources/app.conf file to ~/.edask/conf/app.conf and edit.
Log files will be written to ~/.edask/logs.

#### Startup

The edask/bin directory contains scripts for starting up the EDASK server and a dask cluster.  For example, to start up a dask cluster with 8 worker nodes, one would execute the following:

> startup_cluster.sh 8

Then in a separate shell one would start up the EDAS server by executing:

> startup_server.sh
