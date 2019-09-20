from stratus_endpoint.handler.base import Endpoint, TaskHandle, Status, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import traceback
import atexit, ast, os, json
from edas.portal.base import Message, Response
from typing import Dict, Any, Sequence
from edas.workflow.module import edasOpManager
from edas.portal.parsers import WpsCwtParser
from edas.util.logging import EDASLogger
from edas.process.task import Job
from edas.process.manager import ProcessManager
from edas.stratus.manager import TaskExecHandler
from edas.config import EdasEnv

def get_or_else( value, default_val ): return value if value is not None else default_val

class EDASEndpoint(Endpoint):

    def __init__(self, **kwargs ):
        super(EDASEndpoint, self).__init__()
        self.logger =  EDASLogger.getLogger()
        EdasEnv.update( kwargs )
        self.process = "edas"
        self.handlers = {}
        self.processManager = None
        self._epas = [ "edas*", "xarray*" ]
        atexit.register( self.shutdown, "ShutdownHook Called" )

    def epas( self ) -> List[str]: return self._epas

    def init( self ):
        self.processManager = ProcessManager.initManager( EdasEnv.parms )
        self.scheduler_info = self.processManager.client.scheduler_info()
        self.logger.info(" \n @@@@@@@ SCHEDULER INFO:\n " + str(self.scheduler_info ))

    @staticmethod
    def elem( array: Sequence[str], index: int, default: str = "" )-> str:
         return array[index] if( len(array) > index ) else default

    def capabilities(self, type: str, **kwargs  ) -> Dict:
        if type == "epas":
            return dict( epas = self._epas )
        elif type == "capabilities" or type == "" or type is None:
            if type == None: type = "kernels"
            capabilities = edasOpManager.getCapabilitiesXml(type)
            return Message( type, "capabilities", capabilities ).dict()
        elif type == "processes":
            capabilities = edasOpManager.getCapabilitiesJson(type)
            return Message( type, "capabilities", capabilities ).dict()
        elif type == "util":
            utilSpec = kwargs.get( "spec" )
            (module, op) = WpsCwtParser.split([":", "."], utilSpec[1])
            description = json.dumps( edasOpManager.describeProcess(module, op) )
            return Message(utilSpec[0], "capabilities", description).dict()
        else: raise Exception( f"Unknown capabilities type: {type}" )

    def getVariableSpec(self, collId: str, varId: str  ) -> Dict:
        from edas.collection.agg import Collection
        col = Collection.new( collId )
        varSpec = col.getVariableSpec( varId )
        return  Message( "var", "VariableSpec", varSpec ).dict()

    def execUtility( self, utilSpec: Sequence[str] ) -> Dict:
        uType = utilSpec[0].lower()
        for capType in [ 'col', 'ker' ]:
            if uType.startswith( capType ):
                return self.capabilities( uType )
        if uType.startswith( "var" ):
            if len( utilSpec ) <= 2: raise Exception( "Missing parameter(s) to getVariableSpec" )
            return self.getVariableSpec( utilSpec[1], utilSpec[2]  )
        return Message("","","").dict()

    def addHandler(self, submissionId, handler ):
        self.handlers[ submissionId ] = handler
        return handler

    def removeHandler(self, submissionId ):
        try:
            del self.handlers[ submissionId ]
        except:
            self.logger.error( "Error removing handler: " + submissionId + ", existing handlers = " + str(self.handlers.keys()))

    def parseMap( self, serialized_map: str )-> Dict[str,str]:
        return ast.literal_eval(serialized_map)

    def defaultResponseType( self, runargs:  Dict[str, Any] )-> str:
         status = bool(str(runargs.get("status","false")))
         return "file" if status else "xml"

    def sendErrorReport( self, clientId: str, responseId: str, msg: str ):
        self.logger.info("@@Portal-----> SendErrorReport[" + clientId +":" + responseId + "]: " + msg )

    def sendFile( self, clientId: str, jobId: str, name: str, filePath: str, sendData: bool ):
        self.logger.debug( "@@Portal: Sending file data to client for {}, filePath={}".format( name, filePath ) )

    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        rid: str = kwargs.get('rid', Job.randomStr(4) )
        cid: str = kwargs.get('cid', Job.randomStr(4) )
        self.logger.info( f"EDAS Endpoint--> processing rid {rid}")
        proj = requestSpec.get("proj", "proj-" + Job.randomStr(4) )
        exp = requestSpec.get("exp",  "exp-" + Job.randomStr(4) )
        try:
          job = Job.create( rid, proj, exp, 'exe', requestSpec, inputs, {}, 1.0 )
          execHandler: TaskExecHandler = self.addHandler(rid, TaskExecHandler(cid, job))
          execHandler.execJob( job )
          return execHandler
        except Exception as err:
            self.logger.error( "Caught execution error: " + str(err) )
            traceback.print_exc()
            return TaskHandle(rid=rid, cid=cid, status = Status.ERROR, error = TaskExecHandler.getErrorReport(err))


    def shutdown( self, *args ):
        print( "Shutdown: " + str(args) )
        if self.processManager is not None:
            self.processManager.term()

if __name__ == '__main__':

    CreateIPServer = "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/"
    CIP_addresses = {
        "merra2": CreateIPServer + "/reanalysis/MERRA2/mon/atmos/{}.ncml",
        "merra": CreateIPServer + "/reanalysis/MERRA/mon/atmos/{}.ncml",
        "ecmwf": CreateIPServer + "/reanalysis/ECMWF/mon/atmos/{}.ncml",
        "cfsr": CreateIPServer + "/reanalysis/CFSR/mon/atmos/{}.ncml",
        "20crv": CreateIPServer + "/reanalysis/20CRv2c/mon/atmos/{}.ncml",
        "jra": CreateIPServer + "/reanalysis/JMA/JRA-55/mon/atmos/{}.ncml",
    }
    def CIP( model: str, varName: str) -> str:
        return CIP_addresses[model.lower()].format(varName)

    TAS = [
        'https://aims3.llnl.gov/thredds/dodsC/css03_data/CMIP6/CMIP/NASA-GISS/GISS-E2-1-G/amip/r1i1p1f1/Amon/tas/gn/v20181016/tas_Amon_GISS-E2-1-G_amip_r1i1p1f1_gn_185001-190012.nc',
        'https://aims3.llnl.gov/thredds/dodsC/css03_data/CMIP6/CMIP/NASA-GISS/GISS-E2-1-G/amip/r1i1p1f1/Amon/tas/gn/v20181016/tas_Amon_GISS-E2-1-G_amip_r1i1p1f1_gn_190101-195012.nc',
        'https://aims3.llnl.gov/thredds/dodsC/css03_data/CMIP6/CMIP/NASA-GISS/GISS-E2-1-G/amip/r1i1p1f1/Amon/tas/gn/v20181016/tas_Amon_GISS-E2-1-G_amip_r1i1p1f1_gn_195101-200012.nc',
        'https://aims3.llnl.gov/thredds/dodsC/css03_data/CMIP6/CMIP/NASA-GISS/GISS-E2-1-G/amip/r1i1p1f1/Amon/tas/gn/v20181016/tas_Amon_GISS-E2-1-G_amip_r1i1p1f1_gn_200101-201412.nc',
    ]

    TAS_ESGF = [
        'esgf@https://dataserver.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r2i1p3/clwvi_Amon_GISS-E2-H_historical_r2i1p3_185001-190012.nc'
        'esgf@https://dataserver.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r2i1p3/clwvi_Amon_GISS-E2-H_historical_r2i1p3_190101-195012.nc',
        'esgf@https://dataserver.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r2i1p3/clwvi_Amon_GISS-E2-H_historical_r2i1p3_195101-200512.nc',
    ]

    TAS_NASA = [
        'https://dataserver.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r2i1p3/clwvi_Amon_GISS-E2-H_historical_r2i1p3_185001-190012.nc'
        'https://dataserver.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r2i1p3/clwvi_Amon_GISS-E2-H_historical_r2i1p3_190101-195012.nc',
        'https://dataserver.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r2i1p3/clwvi_Amon_GISS-E2-H_historical_r2i1p3_195101-200512.nc',
    ]

    ep = EDASEndpoint()
    request = dict(
        domain = [{"name": "d0", "lat": {"start": 0, "end": 80, "system": "values"}, "lon": {"start": 40, "end": 60, "system": "values"},  "time": {"start": "1980-01-01", "end":  "1981-12-31", "crs": "timestamps"}} ],
        input = [ {"uri": CIP("merra2","tas"), "name": "tas:v0", "domain": "d0"} ],
        operation = [ { "name": "edas.subset", "input": "v0" } ],
        rid = "test1",
        cid = "C1"
    )
    task = ep.request( request )

    if task.status == Status.COMPLETED:
        result = task.getResult( block = True )
        print( "Received result: " + str(result) )





