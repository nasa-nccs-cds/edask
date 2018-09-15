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

    def successCallback(self, resultFuture: Future):
        print("SUCCESS")

    def failureCallback(self, message: str):
        print("ERROR: "+ message)

    def iterationCallback( self, resultFuture: Future ):
        print("ITERATE")


class AppTests:

    def __init__( self, appConfiguration: Dict[str,str] ):
        self.logger =  logging.getLogger()
        self.processManager = ProcessManager(appConfiguration)


    def exec( self, name, domains: List[Dict[str, Any]], variables: List[Dict[str, Any]], operations: List[Dict[str, Any]] ):
        job = Job.init( name, domains, variables, operations)
        return self.runJob( job )

    def runJob( self, job: Job, clientId: str = "local" )-> Response:
        try:
          resultHandler = AppTestsResultHandler( "local", job.identifier )
          self.processManager.executeProcess(job.identifier, job, resultHandler )
          return Message( clientId, job.identifier, resultHandler.filePath )
        except Exception as err:
            self.logger.error( "Caught execution error: " + str(err) )
            traceback.print_exc()
            return Message( clientId, job.identifier, str(err) )

    def test_detrend(self):
        domains = [ {"name": "d0", "lat": {"start": 0, "end": 50, "system": "values"}, "lon": {"start": 0, "end": 50, "system": "values"}, "time": { "start": '1900-01-01', "end": '1920-01-01', "system":"values" }  },
                    {"name": "d1", "lat": {"start": 20, "end": 20, "system": "values"}, "lon": {"start": 20, "end": 20, "system": "values"}}]
        variables = [{"uri":  TestDataManager.getAddress("merra2", "tas"), "name": "tas:v0", "domain":"d0"}]
        operations = [  {"name": "xarray.decycle", "input": "v0", "result":"dc"},
                        {"name": "xarray.norm", "axis":"xy", "input": "dc", "result":"dt" },
                        {"name": "xarray.subset", "input": "dt", "domain":"d1"} ]
        return self.exec( "test_detrend", domains, variables, operations )

if __name__ == '__main__':
    tester = AppTests({})
    result = tester.test_detrend()
    print(result)




