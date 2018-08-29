import zmq, traceback
from dask.distributed import Future
from edask.workflow.data import EDASDataset
import random, string, os, datetime, atexit
from edask.portal.base import EDASPortal, Message, Response
from typing import List, Dict, Any, Union, Sequence
from edask.workflow.module import edasOpManager
from edask.process.manager import ProcessManager, Job


class ExecResultHandler:

    def __init__(self, portal: EDASPortal, clientId: str, jobId: str, **kwargs ):
        self.clientId = clientId
        self.jobId = jobId
        self.cacheDir = kwargs.get( "cache", "/tmp")
        self.portal = portal
        self.filePath = self.cacheDir + "/" + portal.randomStr(6) + ".nc"

    def successCallback(self, resultFuture: Future):
      status = resultFuture.status
      if status == "finished":
          result: EDASDataset = resultFuture.result()
          filePath = result.save( self.filePath )
          self.portal.sendFile( self.clientId, self.jobId, result.id, filePath, True )
      else:
          self.failureCallback( "status = " + status )
      self.portal.removeHandler( self.clientId, self.jobId )

    def failureCallback(self, message: str):
      self.portal.sendErrorReport( self.clientId, self.jobId, message )
      self.portal.removeHandler( self.clientId, self.jobId )

class EDASapp(EDASPortal):

    @staticmethod
    def elem( array: Sequence[str], index: int, default: str = "" )-> str:
         return array[index] if( len(array) > index ) else default

    def __init__( self, client_address: str="127.0.0.1", request_port: int=4556, response_port: int=4557, appConfiguration: Dict[str,str]={} ):
        super( EDASapp, self ).__init__( client_address, request_port, response_port )
        self.processManager = ProcessManager( appConfiguration )
        self.process = "edas"
        atexit.register( self.term, "ShutdownHook Called" )

    def start( self ): self.run()

    def getCapabilities(self, utilSpec: Sequence[str] ) -> Message:
        capabilities = edasOpManager.getCapabilitiesStr()
        return Message( utilSpec[0], "capabilities", capabilities )


    def describeProcess(self, utilSpec: Sequence[str] ) -> Message:
        ( module, op ) = utilSpec[1].split(":")
        description = edasOpManager.describeProcess( module, op )
        return Message( utilSpec[0], "capabilities", description )


    def execUtility( self, utilSpec: Sequence[str] ) -> Message:
        return Message("","","")

    def getRunArgs( self, taskSpec: Sequence[str] )-> Dict[str,str]:
        runargs: Dict[str,str] = self.parseMap( taskSpec[4] )  if( len(taskSpec) > 4 ) else {}
        responseForm = str( runargs.get("responseform","wps") )
        if responseForm == "wps":
          responseType = runargs.get( "response", self.defaultResponseType(runargs) )
          rv = { k: str(v) for k, v in runargs.items() }
          rv["response" ] = responseType
          return rv
        else:
          responseToks = responseForm.split(':')
          new_runargs = { k: str(v) for k, v in runargs.items() }
          new_runargs["response" ] = responseToks[0]
          if( responseToks[0].lower() == "collection" and len(responseToks) > 1  ): new_runargs["cid"] = responseToks[-1]
          return new_runargs

    def parseMap( self, serialized_map: str )-> Dict[str,str]:
        return {}

    def defaultResponseType( self, runargs:  Dict[str, Any] )-> str:
         status = bool(str(runargs.get("status","false")))
         return "file" if status else "xml"

    def execute( self, taskSpec: Sequence[str] )-> Response:
        clientId = self.elem(taskSpec,0)
        runargs = self.getRunArgs( taskSpec )
        jobId = runargs.get( "jobId",self.randomStr(8) )
        process_name = self.elem(taskSpec,2)
        dataInputsSpec = self.elem(taskSpec,3)
        self.setExeStatus( clientId, jobId, "executing " + process_name + "-> " + dataInputsSpec )
        self.logger.info( " @@E: Executing " + process_name + "-> " + dataInputsSpec + ", jobId = " + jobId + ", runargs = " + str(runargs) )
        try:
          resultHandler: ExecResultHandler = self.addHandler( clientId, jobId, ExecResultHandler( self, clientId, jobId ) )
          self.processManager.executeProcess(jobId, Job( jobId, process_name, dataInputsSpec, runargs, 1.0 ), resultHandler.successCallback, resultHandler.failureCallback)
          return Message( clientId, jobId, resultHandler.filePath )
        except Exception as err:
            self.logger.error( "Caught execution error: " + str(err) )
            traceback.print_exc()
            return Message( clientId, jobId, str(err) )



    # def sendErrorReport( self, clientId: str, responseId: str, exc: Exception ):
    #     err = WPSExceptionReport(exc)
    #     self.sendErrorReport( clientId, responseId, err.toXml() )
    #
    # def sendErrorReport( self, taskSpec: Sequence[str], exc: Exception ):
    #     clientId = taskSpec[0]
    #     runargs = self.getRunArgs( taskSpec )
    #     syntax = self.getResponseSyntax(runargs)
    #     err = WPSExceptionReport(exc)
    #     return self.sendErrorReport( clientId, "requestError", err.toXml() )


    def shutdown(self):
        self.processManager.term()

    def sendFileResponse( self, clientId: str, jobId: str, response: str  ) -> Dict[str,str]:
        return {}


    def sendDirectResponse( self, clientId: str, responseId: str, response: str ) -> Dict[str,str]:
        return {}

if __name__ == "__main__":
    server = EDASapp()
    server.run()