from typing import Dict, Any, Union, List, Callable
import zmq, traceback, time, logging, xml, cdms2, socket, defusedxml, abc
from xml.etree.ElementTree import Element, ElementTree
from threading import Thread
from edask.workflow.module import edasOpManager
from edask.process.task import Job
from edask.workflow.data import EDASDataset
from edask.portal.base import EDASPortal, Message, Response
from dask.distributed import Client, Future, LocalCluster
import random, string, os, queue, datetime, atexit, multiprocessing
from enum import Enum
import xarray as xa

class ResultHandler:
    __metaclass__ = abc.ABCMeta

    def __init__(self, clientId: str, jobId: str, **kwargs ):
        self.clientId = clientId
        self.jobId = jobId
        self.cacheDir = kwargs.get( "cache", "/tmp")
        self.iterations = kwargs.get( "iterations", 1 )
        self.completed = 0
        self.filePath = self.cacheDir + "/" + Job.randomStr(6) + ".nc"

    @abc.abstractmethod
    def successCallback(self, resultFuture: Future): pass

    @abc.abstractmethod
    def failureCallback(self, ex: Exception): pass

    @abc.abstractmethod
    def iterationCallback( self, resultFuture: Future ): pass

class ExecResultHandler(ResultHandler):

    def __init__(self, portal: EDASPortal, clientId: str, jobId: str, **kwargs ):
        super(ExecResultHandler,self).__init__( clientId, jobId, **kwargs )
        self.portal = portal
        self.results: List[EDASDataset] = []

    def successCallback(self, resultFuture: Future):
      status = resultFuture.status
      if status == "finished":
          self.processFinalResult( resultFuture.result() )
      else:
          self.failureCallback( Exception("status = " + status) )
      self.portal.removeHandler( self.clientId, self.jobId )

    def processFinalResult(self, result: EDASDataset ):
        filePath = result.save( self.filePath )
        self.portal.sendFile( self.clientId, self.jobId, result.id, filePath, True )

    def failureCallback(self, ex: Exception ):
      self.portal.sendErrorReport( self.clientId, self.jobId, str(ex) )
      self.portal.removeHandler( self.clientId, self.jobId )

    def iterationCallback( self, resultFuture: Future ):
      status = resultFuture.status
      if status == "finished":
          self.completed = self.completed + 1
          result: EDASDataset = resultFuture.result()
          self.results.append(result)
      else:
          self.failureCallback( Exception("status = " + status) )

      if self.completed == self.iterations:
        self.processFinalResult( EDASDataset.merge( self.results ) )
        self.portal.removeHandler( self.clientId, self.jobId )

class GenericProcessManager:
  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def executeProcess( self, service: str, job: Job, resultHandler: ExecResultHandler )-> str: pass

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
      self.nWorkers = int( self.config.get("nWorkers",multiprocessing.cpu_count()) )
      self.logger =  logging.getLogger()
      self.cluster = LocalCluster( n_workers=self.nWorkers )
      self.client = Client(self.cluster)
      self.client.submit( lambda x: edasOpManager.buildIndices( x ), self.nWorkers )

  def term(self):
      self.client.close()
      self.cluster.close()

  def executeProcess( self, service: str, job: Job, resultHandler: ResultHandler ):
      try:
        self.logger.info( "Defining workflow, nIter = " + str(resultHandler.iterations) )
        if resultHandler.iterations > 1:
            jobs = [ job.copy(wIndex) for wIndex in range(resultHandler.iterations) ]
            result_futures = self.client.map( lambda x: edasOpManager.buildTask( x ), jobs )
            for result_future in result_futures: result_future.add_done_callback( resultHandler.iterationCallback )
        else:
            result_future = self.client.submit( lambda x: edasOpManager.buildTask( x ), job )
            result_future.add_done_callback( resultHandler.successCallback )
        self.logger.info("Submitted computation")

      except Exception as ex:
          self.logger.error( "Execution error: " + str(ex))
          resultHandler.failureCallback( ex )
          traceback.print_exc()

      # request: TaskRequest = TaskRequest( job.requestId, job.identifier, dataInputsObj )
      #
      # serviceProvider = apiManager.getServiceProvider("edas")
      # ( job.requestId, serviceProvider.executeProcess( request, job.datainputs, job.runargs, executionCallback ) )


#
#
#   def alloc = if( _apiManagerOpt.isEmpty ) { _apiManagerOpt = Some( new APIManager( serverConfiguration ) ) }
#   def apiManager: APIManager = { alloc; _apiManagerOpt.get }
#   def serverIsDown: bool = { false; }
#
#   def unacceptable(msg: str): Unit = {
#     logger.error(msg)
#     throw new NotAcceptableException(msg)
#   }
#
#   def term = _apiManagerOpt.foreach( _.shutdown )
#
#   def describeProcess(service: str, name: str, runArgs: Dict[str,str]): xml.Elem = {
#     val serviceProvider = apiManager.getServiceProvider(service)
#     //        logger.info("Executing Service {}, Service provider = {} ".format( service, serviceProvider.getClass.getName ))
#     serviceProvider.describeWPSProcess( name, runArgs )
#   }
#
#   def getCapabilities(service: str, identifier: str, runArgs: Dict[str,str]): xml.Elem = {
#     edasOpManager
#   }
#
#
# //  def getResultFilePath( service: str, resultId: str, executor: WorkflowExecutor ): Option[str] = {
# //    val serviceProvider = apiManager.getServiceProvider(service)
# //    val path = serviceProvider.getResultFilePath( resultId, executor )
# //    logger.info( "EDAS ProcessManager-> getResultFile: " + resultId + ", path = " + path.getOrElse("NULL") )
# //    path
# //  }
#
#   def hasResult( service: str, resultId: str ): bool = { false }
#
#   def getResult( service: str, resultId: str, response_syntax: wps.ResponseSyntax.Value ): Element = {
#     logger.info( "EDAS ProcessManager-> getResult: " + resultId)
#     val serviceProvider = apiManager.getServiceProvider(service)
#     serviceProvider.getResult( resultId, response_syntax )
#   }
#
#   def getResultVariable( service: str, resultId: str ): Option[RDDTransientVariable] = {
#     logger.info( "EDAS ProcessManager-> getResult: " + resultId)
#     val serviceProvider = apiManager.getServiceProvider(service)
#     serviceProvider.getResultVariable(resultId)
#   }
#
#   def getResultVariables( service: str ): Iterable[str] = {
#     val serviceProvider = apiManager.getServiceProvider(service)
#     serviceProvider.getResultVariables
#   }
#
#   def getResultStatus( service: str, resultId: str, response_syntax: wps.ResponseSyntax.Value ): Element = {
#     logger.info( "EDAS ProcessManager-> getResult: " + resultId)
#     val serviceProvider = apiManager.getServiceProvider(service)
#     serviceProvider.getResultStatus(resultId,response_syntax)
#   }
# }

