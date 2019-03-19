### EDAS

##### Earth Data Analytic Services Framework

EDAS is a high performance big data analytics and machine learning framework. This framework enables scientists to execute data processing workflows combining common analysis and forecast operations close to the massive data stores at NASA. The data is accessed in standard (NetCDF, HDF, etc.) formats in a POSIX file system and processed using vetted tools of earth data science, e.g. ESMF, CDAT, NCO, Keras, Tensorflow, etc.  EDAS facilitates the construction of high performance parallel workflows by combining canonical analytic operations to enable processing of huge datasets within limited memory spaces with interactive response times. EDAS services are accessed via a WPS API being developed in collaboration with the ESGF Compute Working Team to support server-side analytics for ESGF. Client packages in Python, Java/Scala, or JavaScript contain everything needed to build and submit EDAS requests.   

#### Installation

Create Conda env:
```
    > conda create -n edas -c conda-forge python=3.6 bokeh bottleneck dask dateparser defusedxml distributed eofs keras libnetcdf matplotlib netCDF4 nco paramiko pillow pydap pyparsing pytest python-graphviz pyzmq scikit-learn scipy xarray zeromq cartopy 
    > source activate edas
```    
Install EDAS:
```
    (edas)> git clone https://github.com/nasa-nccs-cds/edask.git edas
    (edas)> cd edas
    (edas)> python setup.py install
```

#### Configuration

The edas configuration files are found in the edas/resources direcory.
To set up a cluster, copy the edas/resources/hosts.template file to edas/resources/hosts and edit to enter the names of the workers in your cluster.
To setup EDAS server configuration, copy the edas/resources/app.conf file to ~/.edas/conf/app.conf and edit.
Log files will be written to ~/.edas/logs.

#### Startup

The edas/bin directory contains scripts for starting up the EDASK server.

     * By default the scheduler runs on the head node as a subprocess from the server.
     * The script edas/bin/startup_server.sh starts up the server, which in turn starts up the scheduler and workers.
     * The workers are started up using ssh to the nodes in the hosts file.
     * The scheduler and workers are shut down by the server when it shuts down.
     * To manage the scheduler (cluster) outside of edas, set the edas.manage.scheduler (edas.manage.cluster) parameter to false.

#### Parameters
  The following parameters are defined in the **app.conf** file which is created in the *~/.edas/conf/* directory during the configuration process.
```
* wps.server.address:  The network address of the wps web application server -CDWPS- front end (default: "127.0.0.1")
* request.port:        The port on the EDASK head node for the request socket (default: 4556)
* trusted.dap.servers: Comma-separated whitelist of trusted OpenDAP servers, e.g. "https://aims3.llnl.gov/thredds/dodsC"
* response.port:       The port on the EDASK head node for the response socket (default: 4557)
* sources.allowed:     Comma-separated list of allowed input sources, possible values: collection, http, file
* dap.engine:          Python package used for dap access, possible values: pydap, netcdf4
* dask.scheduler:      The network address of the dask scheduler node (typically the first worker node)
* cache.size.max:      Max size in bytes of internal variable cache (default: 500M)
* edas.transients.dir: Directory for EDASK temporary saved files ( default: /tmp ) 
* edas.coll.dir:       Directory containing EDASK collection definition files ( default: ~/.edas )
* esgf.openid:         OpenID for ESGF authentication.
* esgf.password:       Password for ESGF authentication.
* esgf.username:       Username for ESGF authentication.
* edas.manage.cluster:  Allow edas to start up & shut down the cluster (using ssh to nodes in the hosts file)
* edas.manage.scheduler Allow edas to start up & shut down the scheduler (as a subprocess on the head node)
```
