from edask.process.test import DistributedTestManager
from edask.workflow.data import EDASDataset
import numpy.ma as ma
import xarray as xa
import logging, time

class CacheTESTS:

    def __init__(self):
        self.logger =  logging.getLogger()
        self.mgr = DistributedTestManager( "PlotTESTS", "demo" )

    def print(self, results: EDASDataset):
      for variable in results.inputs:
        result = variable.xr.load()
        self.logger.info("\n\n ***** Result {}, shape = {}".format(result.name, str(result.shape)))
        self.logger.info(result)

    def cache_data(self):
        domains = [{ "name": "d0" }]
        variables = [{"uri": "collection:cip_20crv2c_mth", "name": "ts", "domain": "d0"}]
        operations = [  {"name": "xarray.cache", "input": "ts", "result":"cip.20crv.ts" }  ]
        self.mgr.testExec(domains, variables, operations)

    def use_cache(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{ "uri": "collection:cip_20crv2c_mth", "name": "ts:cip.20crv.ts", "domain": "d0", "cache":"opt" } ]
        operations = [  {"name": "xarray.ave", "axis":"t", "input": "cip.20crv.ts" } ]
        result = self.mgr.testExec(domains, variables, operations)
        self.print( result )

if __name__ == '__main__':
    tester = CacheTESTS()
    tstart = time.time()
    tester.cache_data()
    result = tester.use_cache()
    print( " Completed computation in " + str(time.time() - tstart) + " seconds" )
