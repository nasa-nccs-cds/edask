from edas.process.test import DistributedTestManager
import time
appConf = { "sources.allowed": "collection,https", "log.metrics": "true"}

def test_ave_timeslice():
    domains = [{ "name":"d0",   "lat":  { "start":-80, "end":80,  "system":"values" },
                                "time": { "start":'1980-01-01T00Z', "end":'2010-01-01T00Z', "system":"timestamps"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

if __name__ == "__main__":
    mgr = DistributedTestManager("PyTest", "test_suite", appConf)
    time.sleep(10)
    test_ave_timeslice()
