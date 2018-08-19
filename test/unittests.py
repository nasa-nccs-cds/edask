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
            "merra2": CreateIPServer + "/Reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos/{}.ncml",
            "merra":  CreateIPServer + "/Reanalysis/NASA-GMAO/GEOS-5/MERRA/mon/atmos/{}.ncml",
            "ecmwf":  CreateIPServer + "/Reanalysis/ECMWF/IFS-Cy31r2/mon/atmos/{}.ncml",
            "cfsr":   CreateIPServer + "/Reanalysis/NOAA-NCEP/CFSR/mon/atmos/{}.ncml",
        }

    def getAddress(self, model: str, varName: str ) -> str:
        return self.addresses[model.lower()].format( varName )

    def testParseExec( self, domains: List[Dict[str,str]], variables: List[Dict[str,str]], operations: List[Dict[str,str]] ) -> List[KernelResult]:
        testRequest = l2s( [ "domain = " + dl2s(domains), "variable = " +dl2s(variables), "operation = " +dl2s(operations) ] )
        dataInputs = WpsCwtParser.parseDatainputs( testRequest )
        request: TaskRequest = TaskRequest.new( "requestId", "jobId", dataInputs )
        return edasOpManager.buildRequest( request )

    def testExec( self,  domains: List[Dict[str,Any]], variables: List[Dict[str,Any]], operations: List[Dict[str,Any]]  ) -> List[KernelResult]:
        datainputs = { "domain": domains, "variable": variables, "operation": operations }
        request: TaskRequest = TaskRequest.new( "requestId", "jobId", datainputs )
        return edasOpManager.buildRequest( request )

    def print( self, results: List[KernelResult] ):
        for result in results:
            for variable in result.getVariables():
                result = variable.load()
                self.logger.info( "\n\n ***** Result {}, shape = {}".format( result.name, str( result.shape ) ) )
                self.logger.info( result )

    def equals(self, result: KernelResult, verification_arrays: List[ma.MaskedArray], thresh: float = 0.0001 ) -> bool:
        for idx, result in enumerate( result.getVariables() ):
            a1 = result.to_masked_array(copy=False).flatten()
            a2 = verification_arrays[idx].flatten()
            size = min( a1.size, a2.size )
            diff = ( a1[0:size] - a2[0:size] ).ptp(0)
            self.logger.info(" ***** Result {}, differs from verification by {}".format(result.name, str(diff) ) )
            if diff > 2*thresh: return False
        return True

class TestEdask(unittest.TestCase):

    def setUp(self):
        self.mgr = TestManager()

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
        verification_data = ma.array( [ 262.2724, 265.0825, 268.1237, 275.2687, 280.9459, 286.4841, 288.8701,
                                        287.6568, 283.9872, 277.6257, 269.8073, 266.5727, 265.4136, 266.0101,
                                        268.6991, 274.8372, 280.7108, 286.715, 289.1821, 288.7784, 283.9922,
                                        277.8099, 271.1893, 266.1422, 262.264, 264.3005, 268.3983, 275.8602,
                                        281.3764, 285.9894, 289.224, 287.9005, 283.8038, 276.3279, 270.601,
                                        267.5748, 266.4533, 265.9156, 270.2026, 275.3463, 281.0724  ] )
        domains = [{ "name":"d0",   "lat":  { "start":30, "end":80, "system":"values" },
                                    "lon":  { "start":0, "end":100, "system":"values" },
                                    "time": { "start":0, "end":40, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
        operations = [ { "name":"xarray.ave", "input":"v0", "domain":"d0", "axes":"xy" } ]
        results = self.mgr.testExec( domains, variables, operations )
        self.assertTrue( self.mgr.equals( results[0], [ verification_data ] ) )

if __name__ == '__main__':
    unittest.main(verbosity=3)