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

def test_mean(mgr,dataset,variable,time_range):
    print( f"Executing Time average on var {variable} in dataset {dataset}, time range = {time_range}")
    domains = [{ "name":"d0",  "time": {"start": time_range[0], "end": time_range[1], "crs": "timestamps"}  } ]
    variables = [ { "uri": mgr.getAddress( dataset, variable ), "name":f"{variable}:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.mean", "input":"v0", "axes":"t" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_full_mean( mgr, dataset, variable ):
    print( f"Executing Time average on var {variable} in dataset {dataset}")
    domains = [{ "name":"d0" } ]
    variables = [ { "uri": mgr.getAddress( dataset, variable ), "name":f"{variable}:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.mean", "input":"v0", "axes":"t" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

if __name__ == "__main__":
    mgr = LocalTestManager( "PyTest", __file__, appConf ) if LOCAL_TESTS else DistributedTestManager( "PyTest",  __file__, { **appConf, "scheduler.address":"edaskwndev01:8786" } )
    dataset = "merra2-6hr"
    variable = "tas"
    time_range_30y = [ "1981-01-01", "2011-01-01"]
    time_range_35y = [ "1981-01-01", "2014-12-31"]
    time_range_10y = [ "1981-01-01", "1991-01-01" ]
    time_range_1y  = [ "1991-01-01", "1992-01-01"]
    time_range_6m =  [ "1981-01-01", "1981-01-06"]
    test_full_mean( mgr, dataset, variable )


