from edask.process.test import TestManager
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xa
import time, traceback, logging, inspect
import numpy.ma as ma
mgr = TestManager( "PyTest", "test_suite" )

def test_subset():
    verification_data = ma.array([271.715, 271.7168, 271.7106, 271.7268, 270.9894, 270.9614, 270.9766, 271.0617,
                                  270.5978, 270.5309, 270.494, 270.6829, 270.0909, 270.1363, 270.1072, 270.1761,
                                  269.7368, 269.7775, 269.7706, 269.7447, 269.4521, 269.5128, 269.4986, 269.4689,
                                  269.3369, 269.2667, 269.2823, 269.3115, 269.3589, 269.2058, 269.1493, 269.1711])
    domains = [{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                "lon": {"start": 40, "end": 42, "system": "values"},
                "time": {"start": 10, "end": 15, "system": "indices"}}]
    variables = [{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0"}]
    operations = [{"name": "xarray.subset", "input": "v0", "domain": "d0"}]
    results = mgr.testExec(domains, variables, operations)
    assert mgr.equals(results, [verification_data])

def test_filter():
    domains = [{ "name":"d0",   "lat":  { "start":50, "end":55, "system":"values" },
                                "lon":  { "start":40, "end":42, "system":"values" },
                                "time": { "start":'1980-01-01', "end":'1990-01-01', "system":"values" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.filter", "input":"v0", "domain":"d0", "axis":"t", "sel":"aug"} ]
    results = mgr.testExec( domains, variables, operations )
    print( results.xarrays[0].shape )
    assert  results.xarrays[0].shape[0] == 10

def test_ave1():
    # Verification data: nco_scripts/ave1.sh
    verification_data = ma.array( [ 299.2513, 298.508, 296.9505, 293.9985, 289.3845, 286.9066, 285.6096,
                                    287.5726, 290.1945, 294.1584, 297.4008, 298.9573, 299.912, 298.9509,
                                    296.917, 293.4789, 290.42, 287.2475, 285.871, 286.638, 291.0261  ] )
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":50,  "system":"values" },
                                "lon":  { "start":0, "end":100, "system":"values" },
                                "time": { "start":30, "end":50, "system":"indices" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    assert mgr.equals(results, [verification_data])

def test_ave_op_d0():
    # Verification data: nco_scripts/ave1.sh
    verification_data = ma.array( [ 299.2513, 298.508, 296.9505, 293.9985, 289.3845, 286.9066, 285.6096,
                                    287.5726, 290.1945, 294.1584, 297.4008, 298.9573, 299.912, 298.9509,
                                    296.917, 293.4789, 290.42, 287.2475, 285.871, 286.638, 291.0261  ] )
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":50,  "system":"values" },
                                "lon":  { "start":0, "end":100, "system":"values" },
                                "time": { "start":30, "end":50, "system":"indices" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "domain":"d0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    assert mgr.equals(results, [verification_data])

def test_ave1_double_d0():
    # Verification data: nco_scripts/ave1.sh
    verification_data = ma.array( [ 299.2513, 298.508, 296.9505, 293.9985, 289.3845, 286.9066, 285.6096,
                                    287.5726, 290.1945, 294.1584, 297.4008, 298.9573, 299.912, 298.9509,
                                    296.917, 293.4789, 290.42, 287.2475, 285.871, 286.638, 291.0261  ] )
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":50,  "system":"values" },
                                "lon":  { "start":0, "end":100, "system":"values" },
                                "time": { "start":30, "end":50, "system":"indices" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "domain":"d0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    assert mgr.equals(results, [verification_data])
