from edas.process.test import LocalTestManager
appConf = { "sources.allowed": "collection,https" }
mgr = LocalTestManager( "PyTest", "test_suite", appConf )

def test_ave_timeslice():
    domains = [{ "name":"d0",   "lat":  { "start":-80, "end":80,  "system":"values" },
                                "time": { "start":'1980-01-01T00Z', "end":'2010-01-01T00Z', "system":"timestamps"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_collection_time_ave(collection,variable,time_range):
    domains = [{ "name":"d0",  "time": {"start": time_range[0], "end": time_range[1], "crs": "timestamps"} } ]
    variables = [ { "uri": f"collection://{collection}:", "name":f"{variable}:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"t" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

if __name__ == "__main__":
    collection = "merra2.m2t1nxlnd"
    variable = "LWLAND"
    time_range = [ "1980-01-01", "1980-01-05" ]
    test_collection_time_ave( collection, variable, time_range )

