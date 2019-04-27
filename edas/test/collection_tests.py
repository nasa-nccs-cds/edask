from edas.process.test import LocalTestManager, DistributedTestManager
import time
appConf = { "sources.allowed": "collection,https" }
LOCAL_TESTS = False

def test_ave_timeslice(mgr):
    domains = [{ "name":"d0",   "lat":  { "start":-80, "end":80,  "system":"values" },
                                "time": { "start":'1980-01-01T00Z', "end":'2010-01-01T00Z', "system":"timestamps"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_time_ave(mgr):
    domains = [{ "name":"d0" } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_collection_time_ave(mgr,collection,variable,time_range):
    print( f"Executing Time average on var {variable} in collection {collection}, time range = {time_range}")
    domains = [{ "name":"d0",  "time": {"start": time_range[0], "end": time_range[1], "crs": "timestamps"}  } ]
    variables = [ { "uri": f"collection://{collection}:", "name":f"{variable}:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"t" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_collection_mean(mgr,collection,variable,time_range):
    print( f"Executing Time average on var {variable} in collection {collection}, time range = {time_range}")
    domains = [{ "name":"d0",  "time": {"start": time_range[0], "end": time_range[1], "crs": "timestamps"}  } ]
    variables = [ { "uri": f"collection://{collection}:", "name":f"{variable}:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.mean", "input":"v0", "axes":"tyx" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_asia_time_ave(mgr, collection,variable,time_range):
    print( f"Executing Asia Time average on var {variable} in collection {collection}, time range = {time_range}")
    domains = [{ "name":"d0",   "lat": {"start": 30, "end": 66, "system": "values"}, "lon": {"start": 45, "end": 135, "system": "values"},
                                "time": {"start": time_range[0], "end": time_range[1], "crs": "timestamps"}  } ]
    variables = [ { "uri": f"collection://{collection}:", "name":f"{variable}:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"t" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

if __name__ == "__main__":
    mgr = LocalTestManager( "PyTest", __file__, appConf ) if LOCAL_TESTS else DistributedTestManager( "PyTest",  __file__, { **appConf, "scheduler.address":"edaskwndev01:8786" } )
#    collection = "merra2.m2t1nxlnd"
#    collection="merra2_inst1_2d_int_Nx"
    collection="cip_merra2_6hr-atmos-tas"
#    collection = "cip_eraint_mth"
#    variable = "LWLAND"
#    variable = "KE"
#    collection = "cip_merra2_mth"
#    variable = "ta"
    variable = "tas"
    time_range_30y = [ "1981-01-01", "2011-01-01"]
    time_range_35y = [ "1981-01-01", "2014-12-31"]
    time_range_10y = [ "1981-01-01", "1991-01-01" ]
    time_range_1y  = [ "1991-01-01", "1992-01-01"]
    time_range_6m =  [ "1981-01-01", "1981-01-06"]
    test_collection_mean( mgr, collection, variable, time_range_35y )


