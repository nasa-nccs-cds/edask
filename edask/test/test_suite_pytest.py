from edask.process.test import TestManager
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xa
import time, traceback, logging, inspect
import numpy.ma as ma
def Mgr(): return TestManager( "TestSuite", inspect.stack()[0][3] )

def test_subset():
    mgr = Mgr()
    verification_data = ma.array([271.715, 271.7168, 271.7106, 271.7268, 270.9894, 270.9614, 270.9766, 271.0617,
                                  270.5978, 270.5309, 270.494, 270.6829, 270.0909, 270.1363, 270.1072, 270.1761,
                                  269.7368, 269.7775, 269.7706, 269.7447, 269.4521, 269.5128, 269.4986, 269.4689,
                                  269.3369, 269.2667, 269.2823, 269.3115, 269.3589, 269.2058, 269.1493, 269.1711])
    domains = [{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                "lon": {"start": 40, "end": 42, "system": "values"},
                "time": {"start": 10, "end": 15, "system": "indices"}}]
    variables = [{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
    operations = [{"name": "xarray.subset", "input": "v0", "domain": "d0"}]
    results = mgr.testExec(domains, variables, operations)
    assert mgr.equals(results, [verification_data])