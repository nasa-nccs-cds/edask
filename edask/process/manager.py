from typing import Dict, Any, Union, List, Callable, Optional
import zmq, traceback, time, logging, xml, cdms2, socket, defusedxml, abc
from xml.etree.ElementTree import Element, ElementTree
from edask.workflow.module import edasOpManager
from edask.process.task import Job
from edask.workflow.data import EDASDataset
from edask.portal.base import EDASPortal, Message, Response
from dask.distributed import Client, Future, LocalCluster
from edask.util.logging import EDASLogger
import random, string, os, queue, datetime, atexit, multiprocessing, errno
from threading import Thread
from enum import Enum
import xarray as xa

class ResultHandler:
    __metaclass__ = abc.ABCMeta

    def __init__(self, clientId: str, jobId: str, **kwargs ):
        self.logger = EDASLogger.getLogger()
        self.clientId = clientId
        self.jobId = jobId
        self.cacheDir = kwargs.get( "cache", "/tmp")
        self.workers = kwargs.get( "workers", 1 )
        self.completed = 0
        self.start_time = time.time()
        self._futures: List[Future] = None
        self.filePath = self.cacheDir + "/" + Job.randomStr(6) + ".nc"

    def updateStartTime( self):
        self.start_time = time.time()

    def setFutures(self, futures ) -> "ResultHandler":
        self._futures = futures
        return self

    def getResults( self, timeout = None ) -> List[EDASDataset]:
        assert self._futures is not None, "Can't get reults from unsubmitted handler"
        return [ future.result(timeout) for future in self._futures ]

    def getExpDir(self, proj: str, exp: str ) -> str:
        expDir =  self.cacheDir + "/experiments/" + proj + "/" + exp
        return self.mkDir( expDir )

    def getExpFile(self, proj: str, exp: str, name: str, type: str = "nc" ) -> str:
        expDir =  self.getExpDir( proj, exp )
        return expDir + "/" + name + "-" + Job.randomStr(6) + "." + type

    @abc.abstractmethod
    def successCallback(self, resultFuture: Future): pass

    @abc.abstractmethod
    def failureCallback(self, ex: Exception): pass

    @abc.abstractmethod
    def iterationCallback( self, resultFuture: Future ): pass

    def mkDir(self, dir: str ) -> str:
        try:
            os.makedirs(dir)
        except OSError as e:
            if e.errno != errno.EEXIST: raise
        return dir

class ExecResultHandler(ResultHandler):

    def __init__( self, clientId: str, jobId: str, portal: Optional[EDASPortal]=None, **kwargs ):
        super(ExecResultHandler,self).__init__( clientId, jobId, **kwargs )
        self.portal = portal
        self.results: List[EDASDataset] = []

    def processResult( self, result: EDASDataset ):
        self.results.append( result )
        self._processFinalResult( )
        if self.portal: self.portal.removeHandler( self.clientId, self.jobId )

    def successCallback(self, resultFuture: Future):
      status = resultFuture.status
      if status == "finished":
          self.results.append( resultFuture.result() )
          self.logger.info( " Completed computation " + self.jobId + " in " + str(time.time() - self.start_time) + " seconds" )
          self._processFinalResult( )
      else:
          self.failureCallback( resultFuture.result() )
      if self.portal: self.portal.removeHandler( self.clientId, self.jobId )

    def _processFinalResult( self ):
        assert len(self.results), "No results generated by request"
        try:
            result = self.mergeResults()
            savePath = result.save()
            if self.portal:
                self.portal.sendFile( self.clientId, self.jobId, result.id, savePath, True )
            else:
                self.printResult(savePath)
        except Exception as err:
            self.logger.error( "Error processing final result: " + str(err) )

    def printResult( self, filePath: str ):
        dset = xa.open_dataset(filePath)
        print( str(dset) )

    def mergeResults(self) -> EDASDataset:
        mergeMethod: str = self.results[0]["merge"]
        if mergeMethod is None: return EDASDataset.merge( self.results )
        mergeToks = mergeMethod.split(":")
        return self.getBestResult( mergeToks[0].strip().lower(), mergeToks[1].strip().lower() )

    def getBestResult(self, method: str, parm: str )-> EDASDataset:
        bestResult = None
        bestValue = None
        values = []
        for result in self.results:
            pval = result[parm]
            assert pval, "Error, parameter '{}' not defined in dataset".format( parm )
            values.append(float(pval))
            if bestResult is None or self.compare( method, float(pval), bestValue ):
                bestResult = result
                bestValue = float(pval)
        return bestResult

    def compare(self, method: str, current: float, threshold: float ):
        if method == "min": return current < threshold
        if method == "max": return current > threshold
        raise Exception( "Unknown comparison method: " + method )

    def failureCallback(self, ex: Exception ):
        error_message = str(ex)  if ex.__traceback__  is None else str(ex) + ":\n" +  str(traceback.format_tb( ex.__traceback__ ) )
        if self.portal:
            self.portal.sendErrorReport( self.clientId, self.jobId, error_message )
            self.portal.removeHandler( self.clientId, self.jobId )
        else:
            self.logger.error( error_message )

    def iterationCallback( self, resultFuture: Future ):
      status = resultFuture.status
      if status == "finished":
          self.completed = self.completed + 1
          result: EDASDataset = resultFuture.result()
          self.results.append(result)
      else:
          try:                      self.failureCallback( Exception("status = " + status + "\n" + str( traceback.format_tb(resultFuture.traceback(60)) ) ) )
          except TimeoutError:
              try:                  self.failureCallback( Exception("status = " + status + ", Exception = " + str( resultFuture.exception(60) )  ) )
              except TimeoutError:
                                    self.failureCallback( Exception("status = " + status  ) )
      if self.completed == self.workers:
        self.processFinalResult()
        if self.portal:
            self.portal.removeHandler( self.clientId, self.jobId )

class GenericProcessManager:
  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def submitProcess(self, service: str, job: Job, resultHandler: ExecResultHandler)-> str: pass

  @abc.abstractmethod
  def getResult( self, service: str, resultId: str )-> Element: pass

  @abc.abstractmethod
  def getResultStatus( self, service: str, resultId: str )-> Element: pass

  @abc.abstractmethod
  def hasResult( self, service: str, resultId: str )-> bool: pass

  @abc.abstractmethod
  def serverIsDown( self )-> bool: pass

  @abc.abstractmethod
  def term(self): pass

  def waitUntilJobCompletes( self, service: str, resultId: str  ):
    while( not self.hasResult(service,resultId) ): time.sleep(0.5)


class ProcessManager(GenericProcessManager):

  def __init__( self, serverConfiguration: Dict[str,str] ):
      self.config = serverConfiguration
      self.logger =  EDASLogger.getLogger()
      self.submitters = []
      scheduler = self.config.get( "dask.scheduler", None )
      if scheduler is not None:
          self.logger.info( "Initializing Dask cluster with scheduler {}".format(scheduler) )
          self.client = Client(scheduler)
      else:
          nWorkers = int( self.config.get("dask.nworkers",multiprocessing.cpu_count()) )
          self.logger.info( "Initializing Local Dask cluster with {} workers".format(nWorkers) )
          self.client = Client( LocalCluster( n_workers=nWorkers ) )
          self.client.submit( lambda x: edasOpManager.buildIndices( x ), nWorkers )

  def term(self):
      self.client.close()

  def runProcess( self, job: Job ) -> EDASDataset:
    start_time = time.time()
    try:
        self.logger.info( "Running workflow for requestId " + job.requestId)
        result = edasOpManager.buildTask( job )
        self.logger.info( "Completed workflow in time " + str(time.time()-start_time) )
        return result
    except Exception as err:
        self.logger.error( "Execution error: " + str(err))
        traceback.print_exc()

  def submitProcess( self, service: str, job: Job, resultHandler: ExecResultHandler ) -> EDASDataset:
    start_time = time.time()
    try:
        self.logger.info( "Running workflow for requestId " + job.requestId)
        result = edasOpManager.buildTask( job )
        self.logger.info( "Completed workflow in time " + str(time.time()-start_time) )
        resultHandler.processResult( result )
        return result
    except Exception as err:
        self.logger.error( "Execution error: " + str(err))
        self.logger.error( traceback.format_exc() )
        resultHandler.failureCallback(err)

  def submitProcessAsync( self, job: Job, resultHandler: ExecResultHandler ):
      submitter: SubmissionThread = SubmissionThread( job, resultHandler )
      self.submitters.append( submitter )
      submitter.start()

class SubmissionThread(Thread):

    def __init__(self, job: Job, resultHandler: ExecResultHandler ):
        Thread.__init__(self)
        self.job = job
        self.resultHandler = resultHandler
        self.logger =  EDASLogger.getLogger()

    def run(self):
        start_time = time.time()
        try:
            self.logger.info( "Running workflow for requestId " + self.job.requestId)
            result = edasOpManager.buildTask( self.job )
            self.logger.info( "Completed workflow in time " + str(time.time()-start_time) )
            self.resultHandler.processResult( result )
        except Exception as err:
            self.logger.error( "Execution error: " + str(err))
            self.logger.error( traceback.format_exc() )
            self.resultHandler.failureCallback(err)
