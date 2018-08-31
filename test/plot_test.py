from edask.process.test import TestManager
import matplotlib.pyplot as plt
import xarray as xa

class PlotTESTS:

    def __init__(self):
        self.mgr = TestManager()

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
        operations = [ {"name": "xarray.decycle", "input": "v0"}, {"name": "xarray.noop", "input": "v0"} ]
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
        domains = [{"name": "d0", "lat": {"start": 0, "end": 75, "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "input": "v0", "result":"dc"},
                        {"name": "xarray.detrend", "input": "dc", "wsize":50, "result":"dt" },
                        {"name": "xarray.norm", "input": "dt", "axis": "t", "result": "nt" },
                        {"name": "xarray.eof", "modes": 4, "input": "nt" } ]
        results = self.mgr.testExec(domains, variables, operations)
        print( str( results.ids ) )

if __name__ == '__main__':
    tester = PlotTESTS()
    result = tester.test_eofs()
    plt.show()