from edask.process.test import TestManager
import matplotlib.pyplot as plt
from edask.workflow.data import EDASDataset, EDASArray
import xarray as xa

class PlotTESTS:

    def __init__(self):
        self.mgr = TestManager()

    def eof_plot(self, mtype: str, dset: EDASDataset ):
        results_array = dset.find_arrays(mtype)[0]
        fig, axes = plt.subplots( nrows=2, ncols=2 )
        for iaxis in range(4):
            results_array.sel(mode=iaxis).plot(ax=axes[iaxis//2,iaxis%2])

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
                        {"name": "xarray.eof", "modes": 4, "input": "dt" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "pcs", results )
        self.eof_plot( "eofs", results )

    def test_proxy_nodes(self):
        domains = [{"name": "d0",   "lat":  {"start": 0, "end": 80, "system": "values"},
                                    "time": {"start": '1980-01-01T00', "end": '1985-01-31T00', "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [  {"name": "keras.layer", "input": "v0", "result":"L0"},
                        {"name": "keras.layer", "input": "L0", "result":"L1" },
                        {"name": "keras.layer", "input": "L1", "result":"L2" },
                        {"name": "xarray.ave",  "axis":"t", "input": "L2" } ]
        results = self.mgr.testExec(domains, variables, operations)
        results.plot()

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