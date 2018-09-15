from edask.process.test import TestManager
import matplotlib.pyplot as plt
from edask.workflow.data import EDASDataset
from edask.portal.plotters import plotter
import xarray as xa
import logging

class PlotTESTS:

    def __init__(self):
        self.logger =  logging.getLogger()
        self.mgr = TestManager()

    def eof_plot(self, mtype: str, dset: EDASDataset ):
        for results_array in dset.find_arrays( ".*" + mtype + ".*" ):
            fig, axes = plt.subplots( nrows=2, ncols=2 )
            for iaxis in range(4):
                results_array.sel(m=iaxis).plot(ax=axes[iaxis//2,iaxis%2])

    def graph(self, dset: EDASDataset ):
        vars = dset.xr.variables.values()
        plt.subplots(nrows=1, ncols=len(vars))
        for var in vars:
            if isinstance(var,xa.Variable):
                var.plot()

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

    def test_filter0(self):
        from edask.data.sources.timeseries import TimeIndexer
        domains = [{"name": "d0", "lat": {"start": 50, "end": 50, "system": "values"},
                    "lon": {"start": 100, "end": 100, "system": "values"} ,
                    "time": {"start": '1980-01-01', "end": '1990-01-01', "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [ {"name": "xarray.noop", "input": "v0"} ]
        results = self.mgr.testExec(domains, variables, operations)
        test_array = results.inputs[0]
        xarray = time_axis = test_array.xr
        time_axis = xarray.t
        months = time_axis.dt.month
        indices = TimeIndexer.getMonthIndices("aug")
        print( months )
        print(indices)

    def test_filter1(self):
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":55, "system":"values" },
                                    "lon":  { "start":40, "end":42, "system":"values" },
                                    "time": { "start":'1980-01-01', "end":'1990-01-01', "system":"values" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0", "domain":"d0" } ]
        operations = [ { "name":"xarray.filter", "input":"v0", "domain":"d0", "axis":"t", "sel":"month=aug"} ]
        results = self.mgr.testExec( domains, variables, operations )
        xarray = results.xarrays[0]
        print( "Shape = " +  str( xarray.shape ) )
        print( "Months = " + str( xarray.t.dt.month ) )


    def test_detrend(self):
        domains = [ {"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"}, "time": { "start": '1851-01-01', "end": '2012-01-01', "system":"values" }  },
                    {"name": "d1", "lat": {"start": 50, "end": 50, "system": "values"}, "lon": {"start": 100, "end": 100, "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain":"d0"}]
        operations = [  {"name": "xarray.decycle", "input": "v0", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.subset", "input": "dt", "domain":"d1"} ]
        results = self.mgr.testExec(domains, variables, operations)
        print( results.xr )
        results.plot()

    def compute_pcs_SN(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1851-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "result":"modes" },
                        {"name": "xarray.norm", "axis":"t", "input":"modes:pc", "result": "modesn"},
                        {"name": "xarray.archive", "proj":"globalPCs", "exp":"20crv-ts-SN", "input": "modesn" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "pc", results )

    def compute_pcs_TN(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1851-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.detrend", "axis": "t", "input": "dc", "wsize": 50, "result": "dt"},
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "result":"modes" },
                        {"name": "xarray.norm", "axis":"t", "input":"modes:pc", "result": "modesn"},
                        {"name": "xarray.archive", "proj":"globalPCs", "exp":"20crv-ts-TN", "input": "modesn" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "pc", results )

    def plot_eofs_spatial_norm(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.eof", "modes": 4, "input": "dt" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "pc", results )
        self.eof_plot( "eof", results )

    def plot_eofs_temporal_norm(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},
                    "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [{"name": "xarray.decycle", "axis": "t", "input": "v0", "norm": "true", "result": "dc"},
                      {"name": "xarray.detrend", "axis": "t", "input": "dc", "wsize": 50, "result": "dt"},
                      {"name": "xarray.eof", "modes": 4, "input": "dt"}]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot("pc", results)
        self.eof_plot("eof", results)

    def compute_eofs_reduced(self):
        domains = [{"name": "d0", "lat": {"start": 0, "end": 20, "system": "values"}, "lon": {"start": 0, "end": 20, "system": "values"}, "time": {"start": '1900-01-01T00', "end": '1905-01-01T00', "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.detrend", "axis":"t", "input": "dc", "wsize":50, "result":"dt" },
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "result":"modes" },
                        {"name": "xarray.norm", "axis":"t", "input":"modes:pc", "result": "modesn"},
                        {"name": "xarray.archive", "proj":"globalPCs", "exp":"20crv-ts", "input": "modesn" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "pc", results )
        self.eof_plot( "eof", results )

    def test_monsoon_learning(self):
        domains = [{"name": "d0",  "time": {"start": '1880-01-01T00', "end": '2005-01-01T00', "system": "values"} } ]
        variables = [{"uri": "archive:globalPCs/20crv-ts", "name": "pc:v0", "domain":"d0"}, {"uri": "archive:IITM/monsoon","name":"AI:v1","domain":"d0", "offset":"1y"} ]
        operations = [  {"name": "xarray.filter", "input": "v0", "result": "v0f", "axis":"t", "sel": "aug"},
                        {"name": "keras.layer", "input": "v0f", "result":"L0", "axis":"m", "units":16, "activation":"relu"},
                        {"name": "keras.layer", "input": "L0", "result":"L1", "units":1, "activation":"linear" },
                        {"name": "xarray.norm", "input": "v1", "axis":"t", "result": "dc"},
                        {"name": "xarray.detrend", "input": "dc", "axis":"t", "wsize": 50, "result": "t1"},
                        {"name": "keras.train",  "axis":"t", "input": "L1,t1", "epochs":100, "scheduler:iterations":1, "target":"t1" } ]
        results = self.mgr.testExec( domains, variables, operations )
        plotter.plotPerformance( results, "20crv-ts" )

if __name__ == '__main__':
    tester = PlotTESTS()
    result = tester.test_detrend()
    plt.show()