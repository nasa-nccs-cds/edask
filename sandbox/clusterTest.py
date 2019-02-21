from edas.process.test import DistributedTestManager, ExecHandler
from edas.workflow.data import EDASDataset
from edas.util.logging import EDASLogger
import numpy.ma as ma
import xarray as xa
import logging, time

class ClusterTests:

    def __init__(self):
        self.logger =  EDASLogger.getLogger()
        self.mgr = DistributedTestManager( "PlotTESTS", "demo" )
        self.resultHandler: ExecHandler = None

    def print(self, results: EDASDataset):
      for variable in results.inputs:
        result = variable.xr.load()
        self.logger.info("\n\n ***** Result {}, shape = {}".format(result.name, str(result.shape)))
        self.logger.info(result)

    def compute_eofs_TN(self):
        t0 = time.time()
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": "collection:cip_20crv2c_mth", "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "edas.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "edas.detrend", "axis": "t", "input": "dc", "wsize": 50, "result": "dt"},
                        {"name": "edas.eof", "modes": 4, "input": "dt", "archive":"eofs-20crv-ts-TN"  } ]
        result: EDASDataset = self.mgr.testExec(domains, variables, operations)
        print( "Submitted Request, elapsed: {} sec, result:".format( time.time()-t0 ) )
        self.print(result)

    def compute_eofs_SN(self):
        t0 = time.time()
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": "collection:cip_20crv2c_mth", "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "edas.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "edas.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "edas.eof", "modes": 4, "input": "dt", "archive":"eofs-20crv-ts-SN" } ]
        result: EDASDataset = self.mgr.testExec(domains, variables, operations)
        print( "Submitted Request, elapsed: {} sec, result:".format( time.time()-t0 ) )
        self.print(result)


    def test_subset(self):
        t0 = time.time()
        domains = [{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                    "lon": {"start": 40, "end": 42, "system": "values"},
                    "time": {"start": 10, "end": 15, "system": "indices"}}]
        variables = [{"uri": "collection:cip_merra2_mth", "name": "tas:v0", "domain": "d0"}]
        operations = [ { "name": "edas.subset", "input": "v0" } ]
        result: EDASDataset = self.mgr.testExec(domains, variables, operations)
        print( "Submitted Request, elapsed: {} sec, result:".format( time.time()-t0 ) )
        self.print(result)


    def test_subset_dap(self):
        t0 = time.time()
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":55, "system":"values" },
                                    "lon":  { "start":40, "end":42, "system":"values" },
                                    "time": { "start":10, "end":15, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0"  } ]
        operations = [ { "name":"edas.subset", "input":"v0" } ]
        result: EDASDataset = self.mgr.testExec(domains, variables, operations)
        print( "Submitted Request, elapsed: {} sec, result:".format( time.time()-t0 ) )
        self.print(result)


    def test_average(self):
        t0 = time.time()
        domains =    [ { "name": "d0" } ]
        variables =  [ {"uri": "collection:cip_merra2_mth", "name": "tas:v0", "domain": "d0"} ]
        operations = [ {"name": "edas.ave", "input": "v0", "axis": "xy" } ]
        result: EDASDataset = self.mgr.testExec(domains, variables, operations)
        print( "Submitted Request, elapsed: {} sec, result:".format( time.time()-t0 ) )
        self.print(result)

    def test_mean(self):
        t0 = time.time()
        domains =    [ { "name": "d0" } ]
        variables =  [ {"uri": "collection:cip_merra2_mth", "name": "tas:v0", "domain": "d0"} ]
        operations = [ {"name": "edas.mean", "input": "v0", "axis": "xy" } ]
        result: EDASDataset = self.mgr.testExec(domains, variables, operations)
        print( "Submitted Async Request, elapsed: {} sec".format( time.time()-t0 ) )

    def test_mean_dap(self):
        t0 = time.time()
        domains =    [ { "name": "d0" } ]
        variables =  [ {"uri": self.mgr.getAddress( "merra2", "tas"), "name": "tas:v0", "domain": "d0"} ]
        operations = [ {"name": "edas.mean", "input": "v0", "axis": "xy" } ]
        result: EDASDataset = self.mgr.testExec(domains, variables, operations)
        print( "---------------->>>> Completed Request, elapsed: {} sec".format( time.time()-t0 ) )

if __name__ == '__main__':
    tester = ClusterTests()
    tstart = time.time()
    result = tester.test_mean_dap()
