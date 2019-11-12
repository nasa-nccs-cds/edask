### EDAS

##### Earth Data Analytic Services Framework

EDAS is a high performance big data analytics and machine learning framework. This framework enables scientists to execute data processing workflows combining common analysis and forecast operations close to the massive data stores at NASA. The data is accessed in standard (NetCDF, HDF, etc.) formats in a POSIX file system and processed using vetted tools of earth data science, e.g. ESMF, CDAT, NCO, Keras, Tensorflow, etc.  EDAS facilitates the construction of high performance parallel workflows by combining canonical analytic operations to enable processing of huge datasets within limited memory spaces with interactive response times. EDAS services are accessed via a WPS API being developed in collaboration with the ESGF Compute Working Team to support server-side analytics for ESGF. Client packages in Python, Java/Scala, or JavaScript contain everything needed to build and submit EDAS requests.   

#### Installation

Create Conda env:
```
    > conda create -n edas -c conda-forge python=3.6 bokeh bottleneck dask dateparser decorator defusedxml distributed eofs keras libnetcdf netCDF4 networkx requests  six paramiko pillow pydap pyparsing pytest python-graphviz pyyaml pyzmq scikit-learn scipy  xarray zeromq esmpy dask-jobqueue
    > source activate edas
```   

Regridding support (optional) requires the cdms2 package:
```
    > conda install -c conda-forge cdms2
    
``` 
Install EDAS:
```
    (edas)> git clone https://github.com/nasa-nccs-cds/edask.git edas
    (edas)> cd edas
    (edas)> python setup.py install
```

#### Configuration

The edas configuration files, 'app.conf' and 'hosts' are copied to the edas configuration directory during the first installation of edas.  The edas configuration 
directory is defined by the environment variable EDAS_CONFIG_DIR, and defaults to ~/.edas/conf if that variable is not defined. To set up a cluster, edit the 
'hosts' file to enter the names of the workers in your cluster. To setup EDAS server configuration, edit the app.conf file.  The config parameters are described 
below.  Log files will be written to ~/.edas/logs.

#### Startup

The edas/bin directory contains scripts for starting up the EDASK server:

     * By default the cluster is started with the script EDAS/bin/startup_cluster_distributed.sh.  
     * During the startup the scheduler address will be displayed., e.g. "scheduler node: edaskwndev01:8786"
     * In the edas configuration file (~/.edas/conf/app.conf), add a line defining the scheduler address, e.g. "scheduler.address=edaskwndev01:8786"
     * Startup the edas server using the script EDAS/bin/startup_server.sh. 

#### Parameters
  The following parameters are defined in the **app.conf** file which is created in the *~/.edas/conf/* directory during the configuration process.
```
* wps.server.address:  The network address of the wps web application server -CDWPS- front end (default: "127.0.0.1")
* scheduler.address:   The scheduler address, e.g. "edaskwndev01:8786"
* request.port:        The port on the EDASK head node for the request socket (default: 4556)
* trusted.dap.servers: Comma-separated whitelist of trusted OpenDAP servers, e.g. "https://aims3.llnl.gov/thredds/dodsC"
* response.port:       The port on the EDASK head node for the response socket (default: 4557)
* sources.allowed:     Comma-separated list of allowed input sources, possible values: collection, http, https, file
* cache.size.max:      Max size in bytes of internal variable cache (default: 500M)
* edas.transients.dir: Directory for EDASK temporary saved files ( default: /tmp ) 
* edas.coll.dir:       Directory containing EDASK collection definition files ( default: ~/.edas )
* esgf.openid:         OpenID for ESGF authentication.
* esgf.password:       Password for ESGF authentication.
* esgf.username:       Username for ESGF authentication.
```


