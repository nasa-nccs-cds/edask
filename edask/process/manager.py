from typing import Dict, Any, Union, List, Callable, Optional
import zmq, traceback, time, logging, xml, cdms2, socket, defusedxml, abc
from xml.etree.ElementTree import Element, ElementTree
from threading import Thread
from edask.workflow.module import edasOpManager
from edask.process.task import Job
from edask.workflow.data import EDASDataset
from edask.portal.base import EDASPortal, Message, Response
from dask.distributed import Client, Future, LocalCluster
import random, string, os, queue, datetime, atexit, multiprocessing, errno
from edask.collections.agg import Archive
from enum import Enum
import xarray as xa



class ResultHandler:
    __metaclass__ = abc.ABCMeta

    def __init__(self, clientId: str, jobId: str, **kwargs ):
        self.clientId = clientId
        self.jobId = jobId
        self.cacheDir = kwargs.get( "cache", "/tmp")
        self.workers = kwargs.get( "workers", 1 )
        self.completed = 0
        self.filePath = self.cacheDir + "/" + Job.randomStr(6) + ".nc"

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

    def __init__(self, clientId: str, jobId: str, portal: Optional[EDASPortal]=None, **kwargs ):
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
          self._processFinalResult( )
      else:
          self.failureCallback( Exception("status = " + status) )
      if self.portal: self.portal.removeHandler( self.clientId, self.jobId )

    def _processFinalResult( self ):
        assert len(self.results), "No results generated by request"
        result = self.mergeResults()
        savePath = result.save()
        if self.portal:
            self.portal.sendFile( self.clientId, self.jobId, result.id, savePath, True )
        else:
            self.printResult(savePath)

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
        if self.portal:
            self.portal.sendErrorReport( self.clientId, self.jobId, str(ex) )
            self.portal.removeHandler( self.clientId, self.jobId )
        else:
            print("ERROR: " + str(ex))
            print(traceback.format_exc())

    def iterationCallback( self, resultFuture: Future ):
      status = resultFuture.status
      if status == "finished":
          self.completed = self.completed + 1
          result: EDASDataset = resultFuture.result()
          self.results.append(result)
      else:
          self.failureCallback( Exception("status = " + status) )

      if self.completed == self.workers:
        self.processFinalResult()
        if self.portal:
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
      self.logger =  logging.getLogger()
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
      self.cluster.close()

  def executeProcess( self, service: str, job: Job, resultHandler: ResultHandler ):
      try:
        self.logger.info( "Defining workflow, nWorkers = " + str(resultHandler.workers) )
        if resultHandler.workers > 1:
            jobs = [ job.copy(wIndex) for wIndex in range(resultHandler.workers) ]
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

