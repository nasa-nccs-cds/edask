import unittest
import dask
from dask.distributed import Client
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xr
import time, traceback, logging
import numpy as np
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
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":55, "system":"values" },
                                    "lon":  { "start":40, "end":42, "system":"values" },
                                    "time": { "start":10, "end":15, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
        operations = [ { "name":"xarray.subset", "input":"v0", "domain":"d0"} ]
        results = self.mgr.testExec( domains, variables, operations )
        self.mgr.print( results )
        self.assertTrue( True )

if __name__ == '__main__':
    unittest.main()