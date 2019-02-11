### EDAS

##### Earth Data Analytic Services Framework

EDAS is a high performance big data analytics and machine learning framework. This framework enables scientists to execute data processing workflows combining common analysis and forecast operations close to the massive data stores at NASA. The data is accessed in standard (NetCDF, HDF, etc.) formats in a POSIX file system and processed using vetted tools of earth data science, e.g. ESMF, CDAT, NCO, Keras, Tensorflow, etc.  EDAS facilitates the construction of high performance parallel workflows by combining canonical analytic operations to enable processing of huge datasets within limited memory spaces with interactive response times. EDAS services are accessed via a WPS API being developed in collaboration with the ESGF Compute Working Team to support server-side analytics for ESGF. Client packages in Python, Java/Scala, or JavaScript contain everything needed to build and submit EDAS requests.   

#### Installation

To install:
```
    > conda create -n edas -c conda-forge python=3.6 bokeh bottleneck dask dateparser defusedxml distributed eofs keras libnetcdf matplotlib netCDF4 nco paramiko pillow pydap pyparsing pytest python-graphviz pyzmq scikit-learn scipy xarray zeromq cartopy 
    > source activate edas
    > pip install  sklearn_xarray

    > git clone https://github.com/nasa-nccs-cds/edask.git
    > cd edask
    > python setup.py install

```

#### Configuration

The edas configuration files are found in the edask/resources direcory.
To set up a cluster, copy the edask/resources/hosts.template file to edask/resources/hosts and edit to enter the names of the workers in your cluster.
To setup EDASK server configuration, copy the edask/resources/app.conf file to ~/.edask/conf/app.conf and edit.
Log files will be written to ~/.edask/logs.

#### Startup

The edask/bin directory contains scripts for starting up the EDASK server and a dask cluster.  For example, to start up a dask cluster with 8 worker nodes, one would execute the following:

> startup_cluster.sh 8

Then in a separate shell one would start up the EDAS server by executing:

> startup_server.sh

#### Parameters
  The following parameters are defined in the **app.conf** file which is created in the *~/.edask/conf/* directory during the configuration process.
```
* wps.server.address: The network address of the wps web application server -CDWPS- front end (default: "127.0.0.1")
* request.port:       The port on the EDASK head node for the request socket (default: 4556)
* response.port:      The port on the EDASK head node for the response socket (default: 4557)
* sources.allowed:    Comma-separated list of allowed input sources, possible values: collection, http, file
* dap.engine:         Python package used for dap access, possible values: pydap, netcdf4
* dask.scheduler:     The network address of the dask scheduler node (typically the first worker node)
* cache.size.max:     Max size in bytes of internal variable cache (default: 500M)
* edask.cache.dir:    Direcory for EDASK internal saved files (default: ~/.edask/conf) 
* edask.coll.dir:     Directory containing EDASK collection definition files (defaults to cache dir)
```
