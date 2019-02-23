from typing import Dict, Any, Union, List, Callable, Optional
import zmq, traceback, time, logging, xml, socket, abc, dask, threading, queue
from edas.process.task import Job
from edas.process.manager import SubmissionThread
from edas.workflow.data import EDASDataset
from stratus_endpoint.handler.base import Status, Task
from edas.util.logging import EDASLogger
import xarray as xa

class ExecHandler(Task):

    def __init__( self, submissionId: str, _job: Job, **kwargs ):
        super(ExecHandler, self).__init__( submissionId, **kwargs )
        self.logger = EDASLogger.getLogger()
        self.sthread = None
        self._processResults = True
        self.results = queue.Queue()
        self.job = _job
        self._status = Status.IDLE
        self.start_time = time.time()

    def execJob(self, job: Job ) -> SubmissionThread:
        self._status = Status.EXECUTING
        self.sthread = SubmissionThread( job, self.processResult, self.processFailure )
        self.sthread.start()
        self.logger.info( " ----------------->>> Submitted request for job " + job.requestId )
        return self.sthread

    @property
    def status(self):
        return self._status

    def getResult(self, timeout=None, block=False) ->  Optional[xa.Dataset]:
        edasResult = self.results.get( block, timeout )
        return edasResult.xr if edasResult is not None else None

    def processResult( self, result: EDASDataset ):
        self.results.put( result )
        self._status = Status.COMPLETED
        self.logger.info(" ----------------->>> REQUEST COMPLETED "  )

    @classmethod
    def getTbStr( cls, ex ) -> str:
        if ex.__traceback__  is None: return ""
        tb = traceback.extract_tb( ex.__traceback__ )
        return " ".join( traceback.format_list( tb ) )

    @classmethod
    def getErrorReport( cls, ex ):
        try:
            errMsg = getattr( ex, 'message', repr(ex) )
            return errMsg + ">~>" +  str( cls.getTbStr(ex) )
        except:
            return repr(ex)

    def processFailure(self, ex: Exception):
        error_message = self.getErrorReport( ex )
        self.logger.error( error_message )
        self._status = Status.ERROR
        self._parms["error"] = error_message