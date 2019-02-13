from edas.process.test import LocalTestManager, DistributedTestManager
import numpy.ma as ma
LOCAL_TESTS = True
appConf = { "sources.allowed": "collection,https" }
mgr = LocalTestManager( "PyTest", "test_suite", appConf ) if LOCAL_TESTS else DistributedTestManager( "PyTest", "test_suite", appConf )

def test_ave_timeslice():
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":50,  "system":"values" },
                                "lon":  { "start":0, "end":100, "system":"values" },
                                "time": { "start":'1990-01-01T00Z', "end":'1991-01-01T00Z', "system":"timestamps"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_eave1():
    domains = [{ "name":"d0",   "lat":  { "start":50, "end":70, "system":"values" },
                                "lon":  { "start":30, "end":40, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'1980-12-31T23:00:00', "system":"values" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" }, { "uri": mgr.getAddress( "merra", "tas"), "name":"tas:v1", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0,v1", "axis":"e" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_subset():
    verification_data = ma.array([271.715, 271.7168, 271.7106, 271.7268, 270.9894, 270.9614, 270.9766, 271.0617,
                                  270.5978, 270.5309, 270.494, 270.6829, 270.0909, 270.1363, 270.1072, 270.1761,
                                  269.7368, 269.7775, 269.7706, 269.7447, 269.4521, 269.5128, 269.4986, 269.4689,
                                  269.3369, 269.2667, 269.2823, 269.3115, 269.3589, 269.2058, 269.1493, 269.1711])
    domains = [{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                "lon": {"start": 40, "end": 42, "system": "values"},
                "time": {"start": 10, "end": 15, "system": "indices"}}]
    variables = [{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
    operations = [ { "name": "xarray.subset", "input": "v0" } ]
    results = mgr.testExec(domains, variables, operations)
    assert mgr.equals(results, [verification_data])

def test_filter():
    domains = [{ "name":"d0",   "lat":  { "start":50, "end":55, "system":"values" },
                                "lon":  { "start":40, "end":42, "system":"values" },
                                "time": { "start":'1980-01-01', "end":'1990-01-01', "system":"values" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.filter", "input":"v0", "axis":"t", "sel":"aug"} ]
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
    
def test_max1() :
    # Verification data: nco_scripts/max1.sh
    verification_data = ma.array( [ 309.1635, 309.1169, 312.0971, 311.8346, 307.2101, 302.7792, 301.4748, 300.2946, 301.3716, 303.0497, 304.4346 ] )
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":50, "system":"values" },
                                "lon":  { "start":0, "end":10, "system":"values" },
                                "time": { "start":40, "end":50, "system":"indices" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.max", "input":"v0", "domain":"d0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)
    assert mgr.equals(results, [verification_data])

def test_max_timeslice() :
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":50, "system":"values" },
                                "lon":  { "start":0, "end":10, "system":"values" },
                                "time": { "start":'1990-01-01T00:00:00', "end":'1990-01-01T00:00:00', "system":"values" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.max", "input":"v0", "domain":"d0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_max_lon_slice() :
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":50, "system":"values" },
                                "lon":  { "start":0, "end":0, "system":"values" },
                                "time": { "start":'1990-01-01T00:00:00', "end":'1991-01-01T00:00:00', "system":"values" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.max", "input":"v0", "domain":"d0", "axes":"yt" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_min1() :
    # Verification data: nco_scripts/min1.sh
    verification_data = ma.array( [ 258.1156, 252.1156, 254.8867, 262.4825, 269.1955, 271.6146, 272.5411,
                                    272.7783, 269.4982, 264.5517, 258.8628, 255.9127, 255.4483, 256.3108,
                                    259.9818, 261.6541, 267.3035, 270.9368, 272.0101, 271.9341, 269.5397 ] )
    domains = [{ "name":"d0",   "lat":  { "start":50, "end":100, "system":"indices" },
                                "lon":  { "start":30, "end":120, "system":"indices" },
                                "time": { "start":30, "end":50, "system":"indices" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.min", "input":"v0", "domain":"d0", "axes":"xy" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)
    assert mgr.equals(results, [verification_data])

def test_diff1() :
    domains = [{ "name":"d0",   "lat":  { "start":50, "end":70, "system":"values" },
                                "lon":  { "start":30, "end":40, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'1980-12-31T23:00:00', "system":"values" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" }, { "uri": mgr.getAddress( "merra", "tas"), "name":"tas:v1", "domain":"d0" } ]
    operations = [ { "name":"xarray.diff", "input":"v0,v1" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_diff2() :
    domains = [{ "name":"d0",   "time": { "start":'1980-01-01T00:00:00', "end":'1980-03-30T23:00:00', "system":"values" } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" }, { "uri": mgr.getAddress( "merra", "tas"), "name":"tas:v1", "domain":"d0" } ]
    operations = [ { "name":"xarray.diff", "input":"v0,v1" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_ave2() :
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":10,  "system":"values" },
                                "lon":  { "start":100, "end":110, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'1982-01-30T23:00:00', "system":"values"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "domain":"d0", "axes":"t", "groupby": "t.season" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_decycle() :
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":30,  "system":"values" },
                                "lon":  { "start":100, "end":130, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'1986-01-30T23:00:00', "system":"values"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.decycle", "input":"v0" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_seasonal_cycle() :
    domains = [ {'name': 'd0', 'lat': {'start': 20, 'end': 60, 'crs': 'values'},
                               'lon': {'start': 200, 'end': 260, 'crs': 'values'},
                               'time': {'start': '1980-01-01T00:00:00Z', 'end': '2012-12-31T23:59:00Z', 'crs': 'timestamps'}}]
    variables = [{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
    operations = [ { 'name': "xarray.ave", 'axes': "yx", "input": "v0:v1" } , {'name': "xarray.ave", 'axes': "t", 'groupby': "t.season", "input":"v1" } ]
    results = mgr.testExec( domains, variables, operations )
    for variable in results.inputs:
        result: xa.DataArray = variable.xr.load()
        assert result.shape == (4,)
    mgr.print(results)

def test_seasonal_means() :
    domains = [ {'name': 'd0', 'lat': {'start': 20, 'end': 60, 'crs': 'values'},
                               'lon': {'start': 200, 'end': 260, 'crs': 'values'},
                               'time': {'start': '1980-01-01T00:00:00Z', 'end': '2012-12-31T23:59:00Z', 'crs': 'timestamps'}}]
    variables = [{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
    operations = [ { 'name': "xarray.ave", 'axes': "yx", "input": "v0:v1" } , {'name': "xarray.ave", 'axes': "t", 'resample': "t.season", "input":"v1" } ]
    results = mgr.testExec( domains, variables, operations )
    for variable in results.inputs:
        result: xa.DataArray = variable.xr.load()
        assert result.shape == (133,)
    mgr.print(results)


def test_yearly_time_ave():
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":10,  "system":"values" },
                                "lon":  { "start":100, "end":110, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'2000-01-30T23:00:00', "system":"values"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { 'name': "xarray.ave", 'axes': "t", "groupby": "t.year", "input":"v0" } ]
    results = mgr.testExec( domains, variables, operations )
    for variable in results.inputs:
        result = variable.xr.load()
        assert result.shape == (21,21,17)
    mgr.print(results)

def test_detrend() :
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":30,  "system":"values" },
                                "lon":  { "start":100, "end":130, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'1986-01-30T23:00:00', "system":"values"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [  {"name": "xarray.decycle", "input": "v0", "result":"dc"},
                    {"name": "xarray.detrend", "input": "dc", "axis":"t"},
                    {"name": "xarray.noop", "input": "dc"} ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_ave3() :
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":30,  "system":"values" },
                                "lon":  { "start":100, "end":130, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'1986-01-30T23:00:00', "system":"values"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "domain":"d0", "axes":"t", "resample": "t.season" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_ave_dash_in_input():
    domains = [{ "name":"d0",   "lat":  { "start":0, "end":30,  "system":"values" },
                                "lon":  { "start":100, "end":130, "system":"values" },
                                "time": { "start":'1980-01-01T00:00:00', "end":'1986-01-30T23:00:00', "system":"values"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v-0", "domain":"d0" } ]
    operations = [ { "name":"xarray.ave", "input":"v-0", "domain":"d0", "axes":"t", "resample": "t.season" } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

def test_ave_no_domain() :
    variables = [ { "uri": mgr.getAddress( "merra2", "tas"), "name":"tas:v0" } ]
    operations = [ { "name":"xarray.ave", "input":"v0", "axes":"t", "resample": "t.season" } ]
    results = mgr.testExec( [], variables, operations )
    mgr.print(results)


