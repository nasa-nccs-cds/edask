import unittest
import dask
from dask.distributed import Client
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xr
import time, traceback, logging
import numpy.ma as ma
from edask.workflow.internal.xarray import *
from edask.workflow.module import edasOpManager
from edask.portal.parsers import WpsCwtParser
CreateIPServer = "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/"

def q( item: str ):
    i0 = item.strip()
    return i0 if i0.startswith('"') or i0.startswith('{') else '"' + i0 + '"'
def l2s( items: List[str] ) -> str:
    return "[ " + ", ".join(items) + " ]"
def dl2s( items: List[Dict[str,str]] ) -> str:
    return "[ " + ", ".join([ d2s(i) for i in items ]) + " ]"
def d2s( dict: Dict[str,str] ) -> str:
    return "{ " + ", ".join([ q(k)+":"+q(v) for (k,v) in dict.items() ]) + " }"

class TestManager:

    def __init__(self):
        self.logger = logging.getLogger()
        self.addresses = {
            "merra2": CreateIPServer + "/reanalysis/MERRA2/mon/atmos/{}.ncml",
            "merra":  CreateIPServer + "/reanalysis/MERRA/mon/atmos/{}.ncml",
            "ecmwf":  CreateIPServer + "/reanalysis/ECMWF/mon/atmos/{}.ncml",
            "cfsr":   CreateIPServer + "/reanalysis/CFSR/mon/atmos/{}.ncml",
            "20crv":  CreateIPServer + "/reanalysis/20CRv2c/mon/atmos/{}.ncml",
            "jra":  CreateIPServer + "/reanalysis/JMA/JRA-55/mon/atmos/{}.ncml",
        }

    def getAddress(self, model: str, varName: str ) -> str:
        return self.addresses[model.lower()].format( varName )

    def testParseExec( self, domains: List[Dict[str,str]], variables: List[Dict[str,str]], operations: List[Dict[str,str]] ) -> List[EDASDataset]:
        testRequest = l2s( [ "domain = " + dl2s(domains), "variable = " +dl2s(variables), "operation = " +dl2s(operations) ] )
        dataInputs = WpsCwtParser.parseDatainputs( testRequest )
        request: TaskRequest = TaskRequest.new( "requestId", "jobId", dataInputs )
        return edasOpManager.buildRequest( request )

    def testExec( self,  domains: List[Dict[str,Any]], variables: List[Dict[str,Any]], operations: List[Dict[str,Any]]  ) -> List[EDASDataset]:
        datainputs = { "domain": domains, "variable": variables, "operation": operations }
        request: TaskRequest = TaskRequest.new( "requestId", "jobId", datainputs )
        return edasOpManager.buildRequest( request )

    def print(self, results: List[EDASDataset]):
        for result in results:
            for variable in result.inputs:
                result = variable.data.load()
                self.logger.info( "\n\n ***** Result {}, shape = {}".format( result.name, str( result.shape ) ) )
                self.logger.info( result )

    def equals(self, result: EDASDataset, verification_arrays: List[ma.MaskedArray], thresh: float = 0.0001) -> bool:
        for idx, result in enumerate( result.inputs ):
            a1 = result.data.to_masked_array(copy=False).flatten()
            a2 = verification_arrays[idx].flatten()
            size = min( a1.size, a2.size )
            diff = ( a1[0:size] - a2[0:size] ).ptp(0)
            self.logger.info(" ***** Result {}, differs from verification by {}".format(result.name, str(diff) ) )
            if diff > 2*thresh: return False
        return True

class EDaskTestCase(unittest.TestCase):

    def setUp(self):
        self.mgr = TestManager()

class TestEdask(EDaskTestCase):

    @unittest.skip("parsing test")
    def test_parse_subset1(self):
        domains = [{ "name":"d0",   "lat":  '{ "start":50, "end":55, "system":"values" }',
                                    "lon":  '{ "start":40, "end":42, "system":"values" }',
                                    "time": '{ "start":10,  "end":15, "system":"indices" }' } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
        operations = [ { "name":"xarray.subset", "input":"v0", "domain":"d0"} ]
        results = self.mgr.testParseExec( domains, variables, operations )
        self.mgr.print( results )
        self.assertTrue( True )

    def test_subset1(self):
        # Verification data: nco_scripts/subset1.sh
        verification_data = ma.array( [ 271.715, 271.7168, 271.7106, 271.7268, 270.9894, 270.9614, 270.9766, 271.0617,
                                        270.5978, 270.5309, 270.494, 270.6829,  270.0909, 270.1363, 270.1072, 270.1761,
                                        269.7368, 269.7775, 269.7706, 269.7447,269.4521, 269.5128, 269.4986, 269.4689,
                                        269.3369, 269.2667, 269.2823, 269.3115, 269.3589, 269.2058, 269.1493, 269.1711 ] )
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":55, "system":"values" },
                                    "lon":  { "start":40, "end":42, "system":"values" },
                                    "time": { "start":10, "end":15, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
        operations = [ { "name":"xarray.subset", "input":"v0", "domain":"d0"} ]
        results = self.mgr.testExec( domains, variables, operations )
        self.assertTrue( self.mgr.equals( results[0], [ verification_data ] ) )

    def test_ave1(self):
        # Verification data: nco_scripts/ave1.sh
        verification_data = ma.array( [ 299.2513, 298.508, 296.9505, 293.9985, 289.3845, 286.9066, 285.6096,
                                        287.5726, 290.1945, 294.1584, 297.4008, 298.9573, 299.912, 298.9509,
                                        296.917, 293.4789, 290.42, 287.2475, 285.871, 286.638, 291.0261  ] )
        domains = [{ "name":"d0",   "lat":  { "start":0, "end":50,  "system":"values" },
                                    "lon":  { "start":0, "end":100, "system":"values" },
                                    "time": { "start":30, "end":50, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
        operations = [ { "name":"xarray.ave", "input":"v0", "domain":"d0", "axes":"xy" } ]
        results = self.mgr.testExec( domains, variables, operations )
        self.mgr.print(results)
        self.assertTrue(self.mgr.equals(results[0], [verification_data]))

    def test_max1(self):
        # Verification data: nco_scripts/max1.sh
        verification_data = ma.array( [ 309.1635, 309.1169, 312.0971, 311.8346, 307.2101, 302.7792, 301.4748, 300.2946, 301.3716, 303.0497, 304.4346 ] )
        domains = [{ "name":"d0",   "lat":  { "start":0, "end":50, "system":"values" },
                                    "lon":  { "start":0, "end":10, "system":"values" },
                                    "time": { "start":40, "end":50, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
        operations = [ { "name":"xarray.max", "input":"v0", "domain":"d0", "axes":"xy" } ]
        results = self.mgr.testExec( domains, variables, operations )
        self.mgr.print(results)
        self.assertTrue(self.mgr.equals(results[0], [verification_data]))

    def test_min1(self):
        # Verification data: nco_scripts/min1.sh
        verification_data = ma.array( [ 258.1156, 252.1156, 254.8867, 262.4825, 269.1955, 271.6146, 272.5411,
                                        272.7783, 269.4982, 264.5517, 258.8628, 255.9127, 255.4483, 256.3108,
                                        259.9818, 261.6541, 267.3035, 270.9368, 272.0101, 271.9341, 269.5397 ] )
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":100, "system":"indices" },
                                    "lon":  { "start":30, "end":120, "system":"indices" },
                                    "time": { "start":30, "end":50, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
        operations = [ { "name":"xarray.min", "input":"v0", "domain":"d0", "axes":"xy" } ]
        results = self.mgr.testExec( domains, variables, operations )
        self.mgr.print(results)
        self.assertTrue(self.mgr.equals(results[0], [verification_data]))

class DebugTests(EDaskTestCase):

    def test_diff1(self):
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":70, "system":"values" },
                                    "lon":  { "start":30, "end":40, "system":"values" },
                                    "time": { "start":'1980-01-01T00:00:00', "end":'1980-12-31T23:00:00', "system":"values" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" }, { "uri": self.mgr.getAddress( "merra", "tas"), "name":"tas:v1", "domain":"d0" } ]
        operations = [ { "name":"xarray.diff", "input":"v0,v1" } ]
        results = self.mgr.testExec( domains, variables, operations )
        self.mgr.print(results)



