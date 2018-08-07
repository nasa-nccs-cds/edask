from typing import Dict, Any, Union
import zmq, traceback, time, logging, xml, cdms2, socket, defusedxml, abc
from xml.etree.ElementTree import Element, ElementTree
from threading import Thread
from cdms2.variable import DatasetVariable
from random import SystemRandom
import random, string, os, queue, datetime, atexit
from enum import Enum

class GenericProcessManager:
  __metaclass__ = abc.ABCMeta
  @abc.abstractmethod
  def describeProcess(self, service: str, name: str, runArgs: Dict[str,str])-> Element: pass

  @abc.abstractmethod
  def getCapabilities( self, service: str, identifier: str, runArgs: Dict[str,str])-> Element: pass

  @abc.abstractmethod
  def executeProcess( self,  service: str, job: Job )-> ( str, Element ): pass

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

  def __init_( serverConfiguration: Dict[str,str] ):
    CollectionLoadServices.startService()

  def alloc = if( _apiManagerOpt.isEmpty ) { _apiManagerOpt = Some( new APIManager( serverConfiguration ) ) }
  def apiManager: APIManager = { alloc; _apiManagerOpt.get }
  def serverIsDown: bool = { false; }

  def unacceptable(msg: str): Unit = {
    logger.error(msg)
    throw new NotAcceptableException(msg)
  }

  def term = _apiManagerOpt.foreach( _.shutdown )

  def describeProcess(service: str, name: str, runArgs: Dict[str,str]): xml.Elem = {
    val serviceProvider = apiManager.getServiceProvider(service)
    //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
    serviceProvider.describeWPSProcess( name, runArgs )
  }

  def getCapabilities(service: str, identifier: str, runArgs: Dict[str,str]): xml.Elem = {
    val serviceProvider = apiManager.getServiceProvider(service)
        //        logger.info("Executing Service %s, Service provider = %s ".format( service, serviceProvider.getClass.getName ))
    serviceProvider.getWPSCapabilities( identifier, runArgs )
  }

  def executeProcess( service: str, job: Job, executionCallback: Option[ExecutionCallback] = None ): ( str, xml.Elem ) = {
    val dataInputsObj = if( !job.datainputs.isEmpty ) wpsObjectParser.parseDataInputs( job.datainputs ) else Dict.empty[str, Seq[Dict[str, Any]]]
    val request: TaskRequest = TaskRequest( job.requestId, job.identifier, dataInputsObj )
    val serviceProvider = apiManager.getServiceProvider("edas")
    ( job.requestId, serviceProvider.executeProcess( request, job.datainputs, job.runargs, executionCallback ) )
  }

//  def getResultFilePath( service: str, resultId: str, executor: WorkflowExecutor ): Option[str] = {
//    val serviceProvider = apiManager.getServiceProvider(service)
//    val path = serviceProvider.getResultFilePath( resultId, executor )
//    logger.info( "EDAS ProcessManager-> getResultFile: " + resultId + ", path = " + path.getOrElse("NULL") )
//    path
//  }

  def hasResult( service: str, resultId: str ): bool = { false }

  def getResult( service: str, resultId: str, response_syntax: wps.ResponseSyntax.Value ): Element = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResult( resultId, response_syntax )
  }

  def getResultVariable( service: str, resultId: str ): Option[RDDTransientVariable] = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultVariable(resultId)
  }

  def getResultVariables( service: str ): Iterable[str] = {
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultVariables
  }

  def getResultStatus( service: str, resultId: str, response_syntax: wps.ResponseSyntax.Value ): Element = {
    logger.info( "EDAS ProcessManager-> getResult: " + resultId)
    val serviceProvider = apiManager.getServiceProvider(service)
    serviceProvider.getResultStatus(resultId,response_syntax)
  }
}