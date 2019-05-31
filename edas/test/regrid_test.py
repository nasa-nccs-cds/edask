from edas.process.test import LocalTestManager, DistributedTestManager
import numpy.ma as ma
import xarray as xa
import time
LOCAL_TESTS = True
appConf = { "sources.allowed": "collection,https", "log.metrics": "true"}
mgr = LocalTestManager( "PyTest", "test_suite", appConf ) if LOCAL_TESTS else DistributedTestManager( "PyTest", "test_suite", appConf )
if not LOCAL_TESTS: time.sleep(30)

def test_eave1():
    domains = [{ "name":"d0",   "lat":  { "start":50, "end":70, "system":"values" },
                                "lon":  { "start":30, "end":40, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'1980-12-31T23:00:00', "system":"values" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" }, { "uri": mgr.getAddress( "merra", "tas"), "name":"tas:v1", "domain":"d0" } ]
    operations = [ { "name":"edas.ave", "input":"v0,v1", "axis":"e" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_regrid():
    domains = [ {"name":"d0", "lat":{ "start":0, "end":80, "system":"values" }, "lon":{ "start":0, "end":80, "system":"values" }, "time":{ "start":'1980-01-01T00:00:00', "end":'1980-12-31T23:00:00', "system":"values"} } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"edas.regrid", "input":"v0", "gridder":"uniform~2x2" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_regrid_gaussian():
    domains = [ {"name":"d0", "lat":{ "start":0, "end":80, "system":"values" }, "lon":{ "start":0, "end":80, "system":"values" }, "time":{ "start":'1980-01-01T00:00:00', "end":'1980-12-31T23:00:00', "system":"values"} } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"edas.regrid", "input":"v0", "gridder":"gaussian~90" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

if __name__ == '__main__':
    test_regrid_gaussian()