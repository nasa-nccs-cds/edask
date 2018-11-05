from edask.process.test import DistributedTestManager, ExecResultHandler
from edask.workflow.data import EDASDataset
from edask.util.logging import EDASLogger
import numpy.ma as ma
import xarray as xa
import logging, time

class ClusterTests:

    def __init__(self):
        self.logger =  EDASLogger.getLogger()
        self.mgr = DistributedTestManager( "PlotTESTS", "demo" )
        self.resultHandler: ExecResultHandler = None

    def print(self, results: EDASDataset):
      for variable in results.inputs:
        result = variable.xr.load()
        self.logger.info("\n\n ***** Result {}, shape = {}".format(result.name, str(result.shape)))
        self.logger.info(result)

    def compute_eofs_TN(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": "collection:cip_20crv2c_mth", "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.detrend", "axis": "t", "input": "dc", "wsize": 50, "result": "dt"},
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "archive":"eofs-20crv-ts-TN"  } ]
        self.mgr.testExec(domains, variables, operations)

    def compute_eofs_SN(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": "collection:cip_20crv2c_mth", "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "archive":"eofs-20crv-ts-SN" } ]
        self.mgr.testExec(domains, variables, operations)

    def test_subset(self):
        domains = [{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                    "lon": {"start": 40, "end": 42, "system": "values"},
                    "time": {"start": 10, "end": 15, "system": "indices"}}]
        variables = [{"uri": "collection:cip_merra2_mth", "name": "tas:v0", "domain": "d0"}]
        operations = [ { "name": "xarray.subset", "input": "v0" } ]
        self.resultHandler = self.mgr.testExec(domains, variables, operations)

    def test_subset_dap(self):
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":55, "system":"values" },
                                    "lon":  { "start":40, "end":42, "system":"values" },
                                    "time": { "start":10, "end":15, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0"  } ]
        operations = [ { "name":"xarray.subset", "input":"v0" } ]
        resultHandler = self.mgr.testExec(domains, variables, operations)
        print( "Submitted Request, results = " + str( resultHandler.getResults() ) )


if __name__ == '__main__':
    tester = ClusterTests()
    tstart = time.time()
    result = tester.test_subset()
