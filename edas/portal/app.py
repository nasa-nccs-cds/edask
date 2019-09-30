import traceback
from threading import Thread
import atexit, ast, os, json, time
from edas.portal.base import EDASPortal, Message, Response
from typing import Dict, Any, Sequence
from edas.workflow.module import edasOpManager
from edas.portal.parsers import WpsCwtParser
from edas.portal.cluster import EDASCluster
from edas.process.task import Job
from edas.process.manager import ExecHandler, ProcessManager
from edas.config import EdasEnv

def get_or_else( value, default_val ): return value if value is not None else default_val

class EDASapp(EDASPortal):

    @staticmethod
    def elem( array: Sequence[str], index: int, default: str = "" )-> str:
         return array[index] if( len(array) > index ) else default


    def __init__( self, client_address: str = None, request_port: int = None, response_port: int = None ):
        super( EDASapp, self ).__init__(get_or_else(client_address, EdasEnv.get("wps.server.address", "*")),
                                        get_or_else(request_port, EdasEnv.get("request.port", 4556)),
                                        get_or_else(response_port, EdasEnv.get("response.port", 4557)))
        self.process = "edas"
        self.processManager = None
        atexit.register( self.term, "ShutdownHook Called" )
        self.logger.info( "STARTUP CLUSTER")
        self.processManager = ProcessManager.initManager( EdasEnv.parms )
        self.scheduler_info = self.processManager.client.scheduler_info()
        workers: Dict = self.scheduler_info.pop("workers")
        self.logger.info(" @@@@@@@ SCHEDULER INFO: " + str(self.scheduler_info ))
        self.logger.info(f" N Workers: {len(workers)} " )
        for addr, specs in workers.items(): self.logger.info(f"  -----> Worker {addr}: {specs}" )
        log_metrics = EdasEnv.parms.get("log.cwt.metrics", True )
        if log_metrics:
            self.metricsThread =  Thread( target=self.trackCwtMetrics )
            self.metricsThread.start()

    def trackCwtMetrics(self, sleepTime=1.0):
        isIdle = False
        self.logger.info(f" ** TRACKING CWT METRICS ** ")
        while self.active:
            metrics = self.getCWTMetrics()
            counts = metrics['user_jobs_running']
            if counts['processing'] == 0:
                if not isIdle:
                    self.logger.info(f" ** CLUSTER IS IDLE ** ")
                    isIdle = True
            else:
                isIdle = False
                self.logger.info("   ------------------------- CWT METRICS  -------------------  ------------------------ ")
                for key, value in metrics.items():
                    self.logger.info(f" *** {key}: {value}")
                self.logger.info("   ----------------------- -----------------------------------  ----------------------- ")
                time.sleep(sleepTime)

    def getCWTMetrics(self) -> Dict:
        metrics_data = self.processManager.getCWTMetrics()
        metrics_data['wps_requests'] = len( self.handlers )
        return metrics_data

    def start( self ): self.run()

    def getCapabilities(self, type: str  ) -> Message:
        capabilities: Dict = edasOpManager.getCapabilitiesJson(type)
        return Message( type, "capabilities", json.dumps(capabilities) )

    def getVariableSpec(self, collId: str, varId: str  ) -> Message:
        from edas.collection.agg import Collection
        col = Collection.new( collId )
        varSpec = col.getVariableSpec( varId )
        return  Message( "var", "VariableSpec", varSpec )

    def describeProcess(self, utilSpec: Sequence[str] ) -> Message:
        ( module, op ) = WpsCwtParser.split( [":","."], utilSpec[1] )
        description = edasOpManager.describeProcess( module, op )
        return Message( utilSpec[0], "capabilities", json.dumps( description ) )

    def execUtility( self, utilSpec: Sequence[str] ) -> Message:
        uType = utilSpec[0].lower()
        for capType in [ 'col', 'ker' ]:
            if uType.startswith( capType ):
                return self.getCapabilities( uType )
        if uType.startswith( "var" ):
            if len( utilSpec ) <= 2: raise Exception( "Missing parameter(s) to getVariableSpec" )
            return self.getVariableSpec( utilSpec[1], utilSpec[2]  )
        if uType.startswith( "metrics" ):
            mtype = utilSpec[1].lower()
            metrics = self.getCWTMetrics() if mtype == "cwt" else self.processManager.getProfileData(mtype)
            return Message("metrics", mtype, json.dumps( metrics ) )
        if uType.startswith("health"):
            mtype = utilSpec[1].lower()
            health = self.processManager.getHealth(mtype)
            return Message( "health", mtype, health )
        return Message("","","")

    def getRunArgs( self, taskSpec: Sequence[str] )-> Dict[str,str]:
        runargs: Dict[str,str] = self.parseMap( taskSpec[4] )  if( len(taskSpec) > 4 ) else {}
        responseForm = str( runargs.get("responseform","wps") )
        if responseForm == "wps":
          responseType = runargs.get( "response", self.defaultResponseType(runargs) )
          rv = { k: str(v) for k, v in runargs.items() }
          rv["response"] = responseType
          return rv
        else:
          responseToks = responseForm.split(':')
          new_runargs = { k: str(v) for k, v in runargs.items() }
          new_runargs["response" ] = responseToks[0]
          if( responseToks[0].lower() == "collection" and len(responseToks) > 1  ): new_runargs["cid"] = responseToks[-1]
          return new_runargs

    def parseMap( self, serialized_map: str )-> Dict[str,str]:
        return ast.literal_eval(serialized_map)

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
        runargs["ncores"] = self.processManager.ncores.items()[0][1]
        dataInputsSpec = self.elem(taskSpec,3)
        self.setExeStatus( clientId, jobId, "executing " + process_name + "-> " + dataInputsSpec )
        self.logger.info( " @@E: Executing " + process_name + "-> " + dataInputsSpec + ", jobId = " + jobId + ", runargs = " + str(runargs) )
        try:
          job = Job.new( jobId, proj, exp, process_name, dataInputsSpec, runargs, 1.0 )
          execHandler: ExecHandler = self.addHandler(clientId, jobId, ExecHandler(clientId, job, self, workers=job.workers))
          execHandler.start()
          return Message( clientId, jobId, execHandler.filePath )
        except Exception as err:
            self.logger.error( "Caught execution error: " + str(err) )
            traceback.print_exc()
            return Message( clientId, jobId, str(err) )


    # def runJob( self, job: Job, clientId: str = "local" )-> Response:
    #     try:
    #       execHandler: ExecHandler = self.addHandler(clientId, job.process, ExecHandler(clientId, job, workers=job.workers))
    #       execHandler.execJob( job )
    #       return Message(clientId, job.process, execHandler.filePath)
    #     except Exception as err:
    #         self.logger.error( "Caught execution error: " + str(err) )
    #         traceback.print_exc()
    #         return Message(clientId, job.process, str(err))
    #

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


    def shutdown(self, *args, **kwargs):
        if self.processManager is not None:
            self.processManager.term()

    def sendFileResponse( self, clientId: str, jobId: str, response: str  ) -> Dict[str,str]:
        return {}


    def sendDirectResponse( self, clientId: str, responseId: str, response: str ) -> Dict[str,str]:
        return {}

if __name__ == "__main__":
    server = EDASapp()
    server.run()
