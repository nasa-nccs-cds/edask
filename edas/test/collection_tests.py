from edas.process.test import LocalTestManager, DistributedTestManager
import time
appConf = { "sources.allowed": "collection,https" }
LOCAL_TESTS = False
mgr = LocalTestManager( "PyTest", __file__, appConf ) if LOCAL_TESTS else DistributedTestManager( "PyTest",  __file__, appConf )

def test_ave_timeslice():
    domains = [{ "name":"d0",   "lat":  { "start":-80, "end":80,  "system":"values" },
                                "time": { "start":'1980-01-01T00Z', "end":'2010-01-01T00Z', "system":"timestamps"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_collection_time_ave(collection,variable,time_range):
    print( f"Executing Time average on var {variable} in collection {collection}, time range = {time_range}")
    domains = [{ "name":"d0",   "lat": {"start": 30, "end": 66, "system": "values"}, "lon": {"start": 45, "end": 135, "system": "values"},
                                "time": {"start": time_range[0], "end": time_range[1], "crs": "timestamps"}  } ]
    variables = [ { "uri": f"collection://{collection}:", "name":f"{variable}:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"t" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

if __name__ == "__main__":
    if not LOCAL_TESTS: time.sleep(30)      # Allow cluster to startup
    collection = "merra2.m2t1nxlnd"
    variable = "LWLAND"
    time_range_10y = [ "1980-01-01", "1990-01-01" ]
    time_range_5m =  [ "1980-01-01", "1980-01-05"]
    test_collection_time_ave( collection, variable, time_range_5m )

