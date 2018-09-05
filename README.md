## EDASK

##### Earth data analytics using the Dask / XArray toolkit.

  *NOTE: THIS IS A DEVELOPMENTAL PROJECT.*
  
  *NO LEVEL OF FUNCTIONALITY SHOULD BE ASSUMED.*

#### Installation

To install:
```
    > conda create -n edask -c conda-forge -c cdat python=3.6 cdms2 cdutil cdtime vcs
    > conda activate edask
    > conda install dask distributed -c conda-forge
    > conda install xarray matplotlib scipy bottleneck paramiko netCDF4 defusedxml python-graphviz bokeh pyparsing 
    > conda install -c anaconda zeromq pyzmq
    > conda install -c conda-forge libnetcdf nco eofs
    > conda install  pillow scikit-learn tensorflow keras
    
    > git clone https://github.com/nasa-nccs-cds/edask.git
    > cd edask
    > python setup.py install

```
