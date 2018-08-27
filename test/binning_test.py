from edask.process.test import TestManager

testMgr = TestManager()

domains = [{"name": "d0", "lat": {"start": 0, "end": 10, "system": "values"},
            "lon": {"start": 0, "end": 10, "system": "values"},
            "time": {"start": '1980-01-01T00', "end": '1989-01-01T00', "system": "values"}}]
variables = [{"uri": testMgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"},
             {"uri": testMgr.getAddress("merra", "tas"), "name": "tas:v1", "domain": "d0"}]
operations = [{"name": "xarray.diff", "input": "v0,v1"}]
results = testMgr.testExec(domains, variables, operations)
ds = results[0].xr
# taxis = results[0].inputs[0].data.coords.get("t")
# print( results[0].inputs[0].data.coords.get("t").dt )
print( ds.resample(t='Y').mean() )
