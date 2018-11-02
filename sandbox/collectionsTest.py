from edask.process.test import DistributedTestManager
from edask.workflow.data import EDASDataset
import numpy.ma as ma
import xarray as xa
import logging, time

class CollectionsTESTS:

    def __init__(self):
        self.logger =  logging.getLogger()
        self.mgr = DistributedTestManager( "PlotTESTS", "demo" )

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
        verification_data = ma.array([271.715, 271.7168, 271.7106, 271.7268, 270.9894, 270.9614, 270.9766, 271.0617,
                                      270.5978, 270.5309, 270.494, 270.6829, 270.0909, 270.1363, 270.1072, 270.1761,
                                      269.7368, 269.7775, 269.7706, 269.7447, 269.4521, 269.5128, 269.4986, 269.4689,
                                      269.3369, 269.2667, 269.2823, 269.3115, 269.3589, 269.2058, 269.1493, 269.1711])
        domains = [{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                    "lon": {"start": 40, "end": 42, "system": "values"},
                    "time": {"start": 10, "end": 15, "system": "indices"}}]
        variables = [{"uri": "collection:cip_merra2_mth", "name": "tas:v0", "domain": "d0"}]
        operations = [ { "name": "xarray.subset", "input": "v0" } ]
        results = self.mgr.testExec(domains, variables, operations)
        assert self.mgr.equals(results, [verification_data])


if __name__ == '__main__':
    tester = CollectionsTESTS()
    tstart = time.time()
    result = tester.test_subset()
