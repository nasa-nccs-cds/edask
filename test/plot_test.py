from edask.process.test import TestManager
import matplotlib.pyplot as plt
from edask.workflow.data import EDASDataset
from edask.portal.plotters import plotter
import numpy.ma as ma
import xarray as xa
import logging

class PlotTESTS:

    def __init__(self):
        self.logger =  logging.getLogger()
        self.mgr = TestManager("PlotTESTS","demo")

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
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0"},
                     {"uri": self.mgr.getAddress("merra", "tas"), "name": "tas:v1"}]
        operations = [{"name": "xarray.diff", "domain": "d0", "input": "v0,v1"}]
        return self.mgr.testExec(domains, variables, operations)

    def test_decycle(self):
        domains = [{"name": "d0", "lat": {"start": 50, "end": 50, "system": "values"},
                    "lon": {"start": 100, "end": 100, "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [ {"name": "xarray.decycle", "input": "v0", "norm": "true"}, {"name": "xarray.noop", "input": "v0"} ]
        results = self.mgr.testExec(domains, variables, operations)
        results.plot()

    def test_subset1(self):
        domains = [{"name": "d0",   "lat": {"start": 30, "end": 35, "system": "values"},
                                    "lon": {"start": 100, "end": 105, "system": "values"},
                                    "time": {"start": 50, "end": 55, "system": "indices"} }]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [ {"name": "xarray.subset", "input": "v0"} ]
        results = self.mgr.testExec(domains, variables, operations)
        self.print( results )


    def test_subset(self):
        verification_data = ma.array( [ 271.715, 271.7168, 271.7106, 271.7268, 270.9894, 270.9614, 270.9766, 271.0617,
                                        270.5978, 270.5309, 270.494, 270.6829,  270.0909, 270.1363, 270.1072, 270.1761,
                                        269.7368, 269.7775, 269.7706, 269.7447,269.4521, 269.5128, 269.4986, 269.4689,
                                        269.3369, 269.2667, 269.2823, 269.3115, 269.3589, 269.2058, 269.1493, 269.1711 ] )
        domains = [{ "name":"d0",   "lat":  { "start":50, "end":55, "system":"values" },
                                    "lon":  { "start":40, "end":42, "system":"values" },
                                    "time": { "start":10, "end":15, "system":"indices" } } ]
        variables = [ { "uri": self.mgr.getAddress( "merra2", "tas"), "name":"tas:v0" } ]
        operations = [ { "name":"xarray.subset", "input":"v0", "domain":"d0" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.print( results )

    def test_filter0(self):
        from edask.data.sources.timeseries import TimeIndexer
        domains = [{"name": "d0", "lat": {"start": 50, "end": 50, "system": "values"},
                    "lon": {"start": 100, "end": 100, "system": "values"} ,
                    "time": {"start": '1980-01-01', "end": '1990-01-01', "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [ {"name": "xarray.noop", "input": "v0"} ]
        results = self.mgr.testExec(domains, variables, operations)
        test_array = results.inputs[0]
        xarray = test_array.xr
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

    def test_intersect_error(self):
        domains = [ {"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"}, "time": { "start": '1851-01-01', "end": '1860-01-01', "system":"values" }  },
                    {"name": "d1", "lat": {"start": 50, "end": 50, "system": "values"}, "lon": {"start": 100, "end": 100, "system": "values"}}]
        variables = [{"uri": self.mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain":"d0"}]
        operations = [  {"name": "xarray.decycle", "input": "v0", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.subset", "input": "dt", "domain":"d1"} ]
        results = self.mgr.testExec(domains, variables, operations)
        print( results.xr )
        results.plot()

    def cwt_request_test(self):
        from edask.portal.parsers import WpsCwtParser
        request = """[  variable = [{"domain": "d0", "uri": "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP//reanalysis/MERRA2/mon/atmos/tas.ncml", "id": "tas|c44a5e"}];
                        domain = [{"id": "d0", "time": {"start": "1980-01-01T00:00:00Z", "step": 1, "end": "1980-12-31T23:59:00Z","crs": "timestamps"}}];
                        operation = [{"input": ["c44a5e"], "domain": "d0", "axes": "tyx", "name": "xarray.ave", "result": "bee960"}]
                     ]"""
        dataInputs = WpsCwtParser.parseDatainputs(request)
        domains = dataInputs["domain"]
        operations = dataInputs["operation"]
        variables = dataInputs["variable"]
        results = self.mgr.testExec(domains, variables, operations)
        print(results.xr)

    def compute_pcs_SN(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1851-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "result":"modes" },
                        {"name": "xarray.norm", "axis":"t", "input":"modes:pc", "archive": "pcs-20crv-ts-SN"} ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "pc", results )

    def compute_eofs_SN(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "archive":"eofs-20crv-ts-SN" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "modes", results )

    def compute_eofs_SN_MERRA(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("merra2", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "archive":"eofs-merra2-ts-SN" } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "modes", results )

    def compute_eofs_TN(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1880-01-01T00', "end": '2012-01-01T00', "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.detrend", "axis": "t", "input": "dc", "wsize": 50, "result": "dt"},
                        {"name": "xarray.eof", "modes": 4, "input": "dt", "archive":"eofs-20crv-ts-TN"  } ]
        results = self.mgr.testExec(domains, variables, operations)
        self.eof_plot( "modes", results )

    def compute_pcs_TN(self):
        domains = [{"name": "d0", "lat": {"start": -80, "end": 80, "system": "values"},  "time": {"start": '1851-01-01T00', "end": '2005-12-31T00', "system": "values"} }]
        variables = [{"uri": self.mgr.getAddress("20crv", "ts"), "name": "ts:v0", "domain": "d0"}]
        operations = [  {"name": "xarray.decycle", "axis":"t", "input": "v0", "norm":"true", "result":"dc"},
                        {"name": "xarray.detrend", "axis": "t", "input": "dc", "wsize": 50, "result": "dt"},
                        {"name": "xarray.eof", "modes": 32, "input": "dt", "result":"modes" },
                        {"name": "xarray.norm", "axis":"t", "input":"modes:pcs", "archive":"pcs-20crv-ts-TN" } ]
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

    def test_monsoon_learning(self):
        domains = [{"name": "d0",  "time": {"start": '1880-01-01T00', "end": '2005-01-01T00', "system": "values"} } ]
        variables = [{"uri": "archive:pcs-20crv-ts-TN", "name": "pcs:v0", "domain":"d0"}, {"uri": "archive:IITM/monsoon/timeseries","name":"AI:v1","domain":"d0", "offset":"1y"} ]
        operations = [  {"name": "xarray.filter", "input": "v0", "result": "v0f", "axis":"t", "sel": "aug"},
                        {"name": "keras.layer", "input": "v0f", "result":"L0", "axis":"m", "units":64, "activation":"relu"},
                        {"name": "keras.layer", "input": "L0", "result":"L1", "units":1, "activation":"linear" },
                        {"name": "xarray.norm", "input": "v1", "axis":"t", "result": "dc"},
                        {"name": "xarray.detrend", "input": "dc", "axis":"t", "wsize": 50, "result": "t1"},
                        {"name": "keras.train",  "axis":"t", "input": "L1,t1", "lr":0.002, "vf":0.2, "decay":0.002, "momentum":0.9, "epochs":1000, "batch":200, "iterations":100, "target":"t1", "archive":"model-20crv-ts" } ]
        results = self.mgr.testExec( domains, variables, operations )
        plotter.plotPerformance(results, "20crv-ts")
        plotter.plotPrediction( results, "20crv-ts" )

    def test_network_model(self):
        domains = [{"name": "d0",  "time": {"start": '1880-01-01T00', "end": '2005-01-01T00', "system": "values"} } ]
        variables = [ {"uri": "archive:pcs-20crv-ts-TN", "name": "pcs:v0", "domain":"d0"}, {"uri": "archive:IITM/monsoon/timeseries","name":"AI:v1","domain":"d0", "offset":"1y"} ]
        operations = [  {"name": "xarray.filter", "input": "v0", "result": "v0f", "axis":"t", "sel": "aug"},
                        {"name": "xarray.norm", "input": "v1", "axis": "t", "result": "dc"},
                        {"name": "xarray.detrend", "input": "dc", "axis": "t", "wsize": 50, "product":"target"},
                        { "name": "keras.model", "input": "v0f", "model":"model-20crv-ts", "product":"prediction" } ]
        results = self.mgr.testExec( domains, variables, operations )
        plotter.plotPrediction(results, "20crv-ts")

if __name__ == '__main__':
    tester = PlotTESTS()
    result = tester.test_subset()
    plt.show()