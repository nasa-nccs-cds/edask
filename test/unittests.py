import unittest
from typing import List, Dict, Sequence, Mapping, Any
import xarray as xr
import time, traceback, logging
import numpy.ma as ma
from edask.workflow.internal.xarray import *
from edask.process.test import TestManager
CreateIPServer = "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/"

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

    def test_diff1(self):
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":70, "system":"values" },
                                    "lon":  { "start":30, "end":40, "system":"values" },
                                    "time": { "start":'1980-01-01T00:00:00', "end":'1980-12-31T23:00:00', "system":"values" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" }, { "uri": self.mgr.getAddress( "merra", "tas"), "name":"tas:v1", "domain":"d0" } ]
        operations = [ { "name":"xarray.diff", "input":"v0,v1" } ]
        results = self.mgr.testExec( domains, variables, operations )
        self.mgr.print(results)

    def test_eave1(self):
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":70, "system":"values" },
                                    "lon":  { "start":30, "end":40, "system":"values" },
                                    "time": { "start":'1980-01-01T00:00:00', "end":'1980-12-31T23:00:00', "system":"values" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" }, { "uri": self.mgr.getAddress( "merra", "tas"), "name":"tas:v1", "domain":"d0" } ]
        operations = [ { "name":"xarray.ave", "input":"v0,v1", "axis":"e" } ]
        results = self.mgr.testExec( domains, variables, operations )
        self.mgr.print(results)


class DebugTests(EDaskTestCase):

    def test_ave1(self):
        pass