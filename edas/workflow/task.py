from edas.portal.messageParser import mParse
from typing import List, Dict, Sequence, Any
from edas.process.source import DataSource
import xarray as xa

class Task:
    _axes = [ None, [ "t" ], [ "y", "x" ], [ "t", "y", "x" ], [ "t", "z", "y", "x" ] ]

    @staticmethod
    def parse( task_header:str ):
        headerToks = task_header.split('|')
        taskToks = headerToks[1].split('-')
        opToks = taskToks[0].split('.')
        module = ".".join( opToks[0:2] )
        op = opToks[-1]
        rId = taskToks[1]
        inputs = headerToks[2].split(',')
        metadata = mParse.s2m( headerToks[3] )

        return Task(module, op, rId, inputs, metadata)

    def __init__( self, module: str, op: str, rId: str, inputs: List[str], metadata: Dict[str,Any] ):
        self.module = module
        self.op = op
        self.rId = rId
        self.inputs: Sequence[str] = inputs
        self.metadata: Dict[str,Any] = metadata
        self.axes: List[str] = self._getAxes()
        self.processUrl()


    def processUrl(self):
        url = self.metadata.get("url")
        scheme, path = DataSource.validate( url )
        if url:
            if scheme == "collection":
               self.metadata["collection"] = path.strip("/")
            elif scheme == "http":
               self.metadata["dap"] = url
            elif scheme == "file":
               self.metadata["file"] = path
            else:
                raise Exception( "Unrecognized scheme '{}' in url: {}".format(scheme,url) )

    def _getAxes(self) -> List[str]:
        raw_axes = self.metadata.get("axes", [] )
        if isinstance(raw_axes, str):
            raw_axes = raw_axes.replace(" ","").strip("[]")
            if( raw_axes.find(",") >= 0 ): return raw_axes.split(",")
            else: return list(raw_axes)
        else:
            return raw_axes

    def varNames(self) -> List[str]:
        return list(self.inputs)

    @classmethod
    def _getCoordName( cls, index: int, nAxes: int ) -> str:
        return cls._axes[ nAxes ][ index ]

    @classmethod
    def getCoordMap( cls, variable: xa.Dataset ) -> Dict[str,str]:
        try:
            return { coord.attrs["axis"].lower(): name  for ( name, coord ) in variable.coords.items() }
        except:
            return { cls._getCoordName( index, len(variable.dims) ): variable.dims[index] for index in range( len(variable.dims) ) }

    @classmethod
    def getAxisMap( cls, variable: xa.Dataset ) -> Dict[str,str]:
        try:
            return { name: coord.attrs["axis"].lower()  for ( name, coord ) in variable.coords.items() }
        except:
            return { variable.dims[index]: cls._getCoordName( index, len(variable.dims) ) for index in range( len(variable.dims) ) }

    def hasAxis( self, axis: str ) -> bool:
        return self.axes.count( axis ) > 0

    def mapCoordinates(self, variable: xa.Dataset ) -> xa.Dataset:
        axis_map = self.getAxisMap( variable )
        return variable.rename( axis_map )

    def getMappedVariables(self, dataset: xa.Dataset ) -> List[xa.Dataset]:
        return [ self.mapCoordinates( dataset[varName] ) for varName in self.varNames() ]

    def getResults(self, dataset: xa.Dataset, load = True ) -> List[xa.Dataset]:
        resultNames = dataset.attrs[ "results-" + self.rId ]
        results = [ dataset[varName] for varName in resultNames ]
        if load: map( lambda x: x.load(), results )
        return results

    def getAttr(self, key ) -> Any:
        return self.metadata.get( key )

#    def getKernel(self):
#        return edasOpManager.getKernel(self)



