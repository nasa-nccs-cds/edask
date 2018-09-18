from edask.process.manager import ProcessManager, ResultHandler
from edask.portal.base import Message, Response
from dask.distributed import Future
from typing import Sequence, List, Dict, Mapping, Optional, Any
import matplotlib.pyplot as plt
from edask.workflow.data import EDASDataset
from edask.portal.plotters import plotter
from edask.process.task import Job
from edask.process.test import TestDataManager
import xarray as xa
import logging, traceback

class AppTestsResultHandler(ResultHandler):

    def __init__(self, clientId: str, jobId: str, **kwargs ):
        super(AppTestsResultHandler,self).__init__(clientId,jobId,**kwargs)
        self.results: List[EDASDataset] = []

    def successCallback(self, resultFuture: Future):
        status = resultFuture.status
        print("SUCCESS")
        if status == "finished":
            self.processFinalResult( resultFuture.result() )
        else:
            self.failureCallback( Exception("status = " + status) )

    def processFinalResult(self, result: EDASDataset ):
        filePath = result.save(self.filePath)
        print( "SUCCESS: wrote result to " + filePath )
        self.printResult(filePath)

    def failureCallback(self, ex: Exception ):
        print( "ERROR: "+ str(ex) )
        print( traceback.format_exc() )

    def printResult( self, filePath: str ):
        dset = xa.open_dataset(filePath)
        print( str(dset) )

    def iterationCallback( self, resultFuture: Future ):
      status = resultFuture.status
      print( "ITERATE: status = {}, iteration = {}".format( resultFuture.status, self.completed ) )
      if status == "finished":
          self.completed = self.completed + 1
          result: EDASDataset = resultFuture.result()
          self.results.append(result)
      else:
          self.failureCallback( Exception("status = " + status) )

      if self.completed == self.iterations:
          self.processFinalResult( EDASDataset.merge( self.results ) )

class AppTests:

    def __init__( self, appConfiguration: Dict[str,str] ):
        self.logger =  logging.getLogger()
        self.processManager = ProcessManager(appConfiguration)

    def exec( self, name, domains: List[Dict[str, Any]], variables: List[Dict[str, Any]], operations: List[Dict[str, Any]] )-> Response:
        job = Job.init( name, domains, variables, operations)
        return self.runJob( job )

    def runJob( self, job: Job, clientId: str = "local" )-> Response:
        try:
          resultHandler = AppTestsResultHandler( "local", job.identifier, iterations=job.iterations )
          self.processManager.executeProcess( job.identifier, job, resultHandler )
          return Message( clientId, job.identifier, resultHandler.filePath )
        except Exception as err:
            self.logger.error( "Caught execution error: " + str(err) )
            traceback.print_exc()
            return Message( clientId, job.identifier, str(err) )

    def plot( self, filePath: str ):
        try:
            dset = xa.open_dataset(filePath)
            vars = list( dset.data_vars.values() )
            nplots = len( vars )
            fig, axes = plt.subplots(ncols=nplots)
            self.logger.info( "Plotting {} plots ".format(nplots) )
            if nplots == 1:
                vars[0].plot(ax=axes)
            else:
                for iaxis, result in enumerate( vars ):
                    result.plot(ax=axes[iaxis])
            plt.show()
        except Exception as err:
            self.logger.error( "Error Plotting: {} ".format(str(err)) )

    def test_detrend(self):
        domains = [ {"name": "d0", "lat": {"start": 0, "end": 50, "system": "values"}, "lon": {"start": 0, "end": 50, "system": "values"}, "time": { "start": '1990-01-01', "end": '2000-01-01', "system":"values" }  },
                    {"name": "d1", "lat": {"start": 20, "end": 20, "system": "values"}, "lon": {"start": 20, "end": 20, "system": "values"}}]
        variables = [{"uri":  TestDataManager.getAddress("merra2", "tas"), "name": "tas:v0", "domain":"d0"}]
        operations = [  {"name": "xarray.decycle", "input": "v0", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.subset", "input": "dt", "domain":"d1"} ]
        return self.exec( "test_detrend", domains, variables, operations )

    def test_norm(self):
        domains = [{"name": "d0", "lat": {"start": 20, "end": 40, "system": "values"}, "lon": {"start": 60, "end": 100, "system": "values"}}]
        variables = [{"uri": TestDataManager.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}]
        operations = [ { "name": "xarray.norm", "axis": "xy", "input": "v0" } ]
        return self.exec( "test_detrend", domains, variables, operations )

    def test_monsoon_learning(self):
        domains = [{"name": "d0",  "time": {"start": '1880-01-01T00', "end": '2005-01-01T00', "system": "values"} } ]
        variables = [{"uri": "archive:globalPCs/20crv-ts", "name": "pc:v0", "domain":"d0"}, {"uri": "archive:IITM/monsoon","name":"AI:v1","domain":"d0", "offset":"1y"} ]
        operations = [  {"name": "xarray.filter", "input": "v0", "result": "v0f", "axis":"t", "sel": "aug"},
                        {"name": "keras.layer", "input": "v0f", "result":"L0", "axis":"m", "units":16, "activation":"relu"},
                        {"name": "keras.layer", "input": "L0", "result":"L1", "units":1, "activation":"linear" },
                        {"name": "xarray.norm", "input": "v1", "axis":"t", "result": "dc"},
                        {"name": "xarray.detrend", "input": "dc", "axis":"t", "wsize": 50, "result": "t1"},
                        {"name": "keras.train",  "axis":"t", "input": "L1,t1", "epochs":100, "scheduler:iterations":4, "target":"t1" } ]
        return self.exec( "test_monsoon_learning", domains, variables, operations )

if __name__ == '__main__':
    tester = AppTests( {"nWorkers":"4"} )
    result: Response = tester.test_monsoon_learning()
    print( result )




