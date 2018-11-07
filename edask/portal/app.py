import traceback
import atexit
from edask.portal.base import EDASPortal, Message, Response
from typing import Dict, Any, Sequence
from edask.workflow.module import edasOpManager
from edask.portal.parsers import WpsCwtParser
from edask.process.task import Job
from edask.process.manager import ProcessManager, ExecHandler
from edask.config import EdaskEnv

def get_or_else( value, default_val ): return value if value is not None else default_val

class EDASapp(EDASPortal):

    @staticmethod
    def elem( array: Sequence[str], index: int, default: str = "" )-> str:
         return array[index] if( len(array) > index ) else default

    def __init__( self, client_address: str = None, request_port: int = None, response_port: int = None ):
        super( EDASapp, self ).__init__(get_or_else(client_address, EdaskEnv.get("client.address", "127.0.0.1")),
                                        get_or_else(request_port, EdaskEnv.get("request.port", 4556)),
                                        get_or_else(response_port, EdaskEnv.get("response.port", 4557)))
        self.processManager = ProcessManager(EdaskEnv.parms)
        self.process = "edas"
        atexit.register( self.term, "ShutdownHook Called" )

    def start( self ): self.run()

    def getCapabilities(self, utilSpec: Sequence[str] ) -> Message:
        capabilities = edasOpManager.getCapabilitiesStr()
        return Message( utilSpec[0], "capabilities", capabilities )


    def describeProcess(self, utilSpec: Sequence[str] ) -> Message:
        ( module, op ) = WpsCwtParser.split( [":","."], utilSpec[1] )
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
        jobId = runargs.get( "jobId", self.elem( taskSpec, 2, Job.randomStr(8) ) )
        proj = runargs.get("proj", "proj-" + Job.randomStr(4) )
        exp = runargs.get("exp",  "exp-" + Job.randomStr(4) )
        process_name = self.elem(taskSpec,2)
        dataInputsSpec = self.elem(taskSpec,3)
        self.setExeStatus( clientId, jobId, "executing " + process_name + "-> " + dataInputsSpec )
        self.logger.info( " @@E: Executing " + process_name + "-> " + dataInputsSpec + ", jobId = " + jobId + ", runargs = " + str(runargs) )
        try:
          job = Job.new( jobId, proj, exp, process_name, dataInputsSpec, runargs, 1.0 )
          execHandler: ExecHandler = self.addHandler(clientId, jobId, ExecHandler(clientId, jobId, self, workers=job.workers))
          execHandler.execJob( job )
          return Message( clientId, jobId, execHandler.filePath )
        except Exception as err:
            self.logger.error( "Caught execution error: " + str(err) )
            traceback.print_exc()
            return Message( clientId, jobId, str(err) )


    def runJob( self, job: Job, clientId: str = "local" )-> Response:
        try:
          execHandler: ExecHandler = self.addHandler(clientId, job.process, ExecHandler(clientId, job.process, workers=job.workers))
          execHandler.execJob( job )
          return Message(clientId, job.process, execHandler.filePath)
        except Exception as err:
            self.logger.error( "Caught execution error: " + str(err) )
            traceback.print_exc()
            return Message(clientId, job.process, str(err))


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