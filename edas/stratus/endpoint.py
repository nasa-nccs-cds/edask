from stratus.handlers.endpoint.base import Endpoint
from typing import Sequence, List, Dict, Mapping, Optional, Any
from edas.portal.app import EDASapp
import traceback
import atexit, ast, os, json
from edas.portal.base import EDASPortal, Message, Response
from typing import Dict, Any, Sequence
from edas.workflow.module import edasOpManager
from edas.portal.parsers import WpsCwtParser
from edas.portal.cluster import EDASCluster
from edas.process.task import Job
from edas.process.manager import ExecHandler, ProcessManager
from edas.config import EdaskEnv

def get_or_else( value, default_val ): return value if value is not None else default_val

class EDASEndpoint(Endpoint):

    def __init__(self, **kwargs ):
        super(EDASEndpoint, self).__init__()
        self.process = "edas"
        self.processManager = None
        self.cluster = None
        atexit.register( self.term, "ShutdownHook Called" )

    def epas( self ) -> List[str]: pass

    def init( self ):
        self.cluster = EDASCluster()
        self.processManager = ProcessManager( EdaskEnv.parms, self.cluster )
        self.scheduler_info = self.processManager.client.scheduler_info()
        self.logger.info(" \n @@@@@@@ SCHEDULER INFO:\n " + str(self.scheduler_info ))

    @staticmethod
    def elem( array: Sequence[str], index: int, default: str = "" )-> str:
         return array[index] if( len(array) > index ) else default

    def getCapabilities(self, type: str  ) -> Message:
        capabilities = edasOpManager.getCapabilities(type)
        return Message( type, "capabilities", capabilities )

    def getVariableSpec(self, collId: str, varId: str  ) -> Message:
        from edas.collection.agg import Collection
        col = Collection.new( collId )
        varSpec = col.getVariableSpec( varId )
        return  Message( "var", "VariableSpec", varSpec )

    def describeProcess(self, utilSpec: Sequence[str] ) -> Message:
        ( module, op ) = WpsCwtParser.split( [":","."], utilSpec[1] )
        description = edasOpManager.describeProcess( module, op )
        return Message( utilSpec[0], "capabilities", description )

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
            metrics = self.cluster.getMetrics( mtype)
            return Message("metrics",mtype, json.dumps( metrics ) )
        return Message("","","")

    def parseMap( self, serialized_map: str )-> Dict[str,str]:
        return ast.literal_eval(serialized_map)

    def defaultResponseType( self, runargs:  Dict[str, Any] )-> str:
         status = bool(str(runargs.get("status","false")))
         return "file" if status else "xml"

    def request(self, type: str, **kwargs ) -> Dict:
        if type == "exe":
            jobId = kwargs.get( "jobId", Job.randomStr(8) )
            proj = kwargs.get("proj", "proj-" + Job.randomStr(4) )
            exp = kwargs.get("exp",  "exp-" + Job.randomStr(4) )
            clientId = kwargs.get("client", "")
            try:
              job = Job.new( jobId, proj, exp, 'exe', kwargs, {}, 1.0 )
              execHandler: ExecHandler = self.addHandler(clientId, jobId, ExecHandler(clientId, job, self, workers=job.workers))
              execHandler.execJob( job )
              return dict( jobId=jobId, result=execHandler.filePath )
            except Exception as err:
                self.logger.error( "Caught execution error: " + str(err) )
                traceback.print_exc()
                return dict( jobId=jobId, error=str(err) )

    def shutdown(self):
        self.cluster.shutdown()
        if self.processManager is not None:
            self.processManager.term()




