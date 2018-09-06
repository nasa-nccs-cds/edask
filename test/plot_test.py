from edask.process.test import TestManager
import matplotlib.pyplot as plt
from edask.workflow.data import EDASDataset, EDASArray
import logging
import xarray as xa

class PlotTESTS:

    def __init__(self):
        self.logger =  logging.getLogger()
        self.mgr = TestManager()

    def eof_plot(self, mtype: str, dset: EDASDataset ):
        results_array = dset.find_arrays(mtype)[0]
        fig, axes = plt.subplots( nrows=2, ncols=2 )
        for iaxis in range(4):
            results_array.sel(m=iaxis).plot(ax=axes[iaxis//2,iaxis%2])

    def print(self, results: EDASDataset):
      for variable in results.inputs:
        result = variable.xr.load()
        self.logger.info("\n\n ***** Result {}, shape = {}".format(result.name, str(result.shape)))
        self.logger.info(result)

    def test_diff(self):
        domains = [{"name": "d0", "lat": {"start": -100, "end": 100, "system": "values"},
                    "lon": {"start": 0, "end": 90, "system": "values"},
                    "time": {"start": '1980-01-01T00', "end": '1980-01-31T00', "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"},
                     {"uri": self.mgr.getAddress("merra", "tas"), "name": "tas:v1", "domain": "d0"}]
        operations = [{"name": "xarray.diff", "input": "v0,v1"}]
        return self.mgr.testExec(domains, variables, operations)

    def test_decycle(self):
        domains = [{"name": "d0", "lat": {"start": 50, "end": 50, "system": "values"},
                    "lon": {"start": 100, "end": 100, "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [ {"name": "xarray.decycle", "input": "v0", "norm": "true"}, {"name": "xarray.noop", "input": "v0"} ]
        results = self.mgr.testExec(domains, variables, operations)
        results.plot()

    def test_detrend(self):
        domains = [{"name": "d0", "lat": {"start": 50, "end": 50, "system": "values"},
                    "lon": {"start": 100, "end": 100, "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "input": "v0", "result":"dc"},
                        {"name": "xarray.detrend", "input": "dc", "wsize":50 },
                        {"name": "xarray.noop", "input": "dc"} ]
        results = self.mgr.testExec(domains, variables, operations)
        results.plot()

    def test_eofs(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.detrend", "input": "dc", "wsize":50, "result":"dt" },
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "result":"eofs" },
                        {"name": "xarray.archive", "proj":"test_eofs", "exp":"pcs-eofs", "input": "eofs" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "pcs", results )
        self.eof_plot( "eofs", results )

    def test_proxy_nodes(self):
        variables = [{"uri": "archive:test_eofs/pcs-eofs", "name": "pcs:v0"}]
        operations = [  {"name": "keras.layer", "input": "v0", "result":"L0", "axis":"m", "units":16, "activation":"relu"},
                        {"name": "keras.layer", "input": "L0", "result":"L1", "units":1, "activation":"linear" },
                        {"name": "keras.train",  "axis":"t", "input": "L1", "epochs":100, "scheduler:iterations":100 } ]
        results = self.mgr.testExec( [], variables, operations )
        self.print( results )

    def test_eofs_reduced(self):
        domains = [{"name": "d0",   "lat":  {"start": 0, "end": 30, "system": "values"},
                                    "lon":  {"start": 0, "end": 30, "system": "values"},
                                    "time": {"start": '1980-01-01T00', "end": '1990-01-31T00', "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "input": "v0", "result":"dc"},
                        {"name": "xarray.detrend", "input": "dc", "wsize":50, "result":"dt" },
                        {"name": "xarray.norm", "input": "dt", "axis": "t", "result": "nt" },
                        {"name": "xarray.eof", "modes": 4, "input": "nt" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "pcs", results )
        self.eof_plot( "eofs", results )

if __name__ == '__main__':
    tester = PlotTESTS()
    result = tester.test_proxy_nodes()
    plt.show()