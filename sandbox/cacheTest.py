from edask.process.test import LocalTestManager
from edask.workflow.data import EDASDataset
from edask.util.logging import EDASLogger
import logging, time

class CacheTESTS:

    def __init__(self):
        self.logger =  EDASLogger.getLogger()
        self.mgr = LocalTestManager("TEST","aveTest")

    def print(self, results: EDASDataset):
      for variable in results.inputs:
        result = variable.xr.load()
        self.logger.info("\n\n ***** Result {}, shape = {}".format(result.name, str(result.shape)))
        self.logger.info(result)

    def cache_data(self):
        domains = [{ "name": "d0" }]
        variables = [{"uri": "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml", "name": "tas", "domain": "d0"}]
        operations = [  {"name": "xarray.cache", "input": "tas", "result":"cip.20crv.tas" }  ]
        self.mgr.testExec(domains, variables, operations, False )
        print( "\n\n --------- Completed Cache OP --------- \n\n" )

    def use_cache(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{ "uri": "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml", "name": "tas:cip.20crv.tas", "domain": "d0", "cache":"opt" } ]
        operations = [  {"name": "xarray.ave", "axis":"t", "input": "cip.20crv.tas" } ]
        result = self.mgr.testExec(domains, variables, operations, False )
        self.print( result )

if __name__ == '__main__':
    USE_CACHE = True
    tester = CacheTESTS()
    t0 = time.time()
    if USE_CACHE: tester.cache_data()
    t1 = time.time()
    result = tester.use_cache()
    t2 = time.time()
    print( " Data Access Time: " + str(t1 - t0) + " seconds" )
    print( " Computation Time: " + str(t2 - t1) + " seconds" )
