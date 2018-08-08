from edask.messageParser import mParse
from typing import List, Dict, Sequence, Mapping
import xarray as xr

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

        return Task( module, op, rId, inputs, metadata )

    def __init__( self, module: str, op: str, rId: str, inputs: Sequence[str], metadata: Dict[str,str] ):
        self.module = module
        self.op = op
        self.rId = rId
        self.inputs = inputs
        self.metadata = metadata
        self.axes = self.metadata.get("axes", [])

    def varNames(self) -> List[str]:
        return [ inputId.split('-')[0] for inputId in self.inputs ]

    @classmethod
    def _getCoordName( cls, index: int, nAxes: int ) -> str:
        return cls._axes[ nAxes ][ index ]

    @classmethod
    def getCoordMap( cls, variable: xr.DataArray ) -> Dict[str,str]:
        try:
            return { coord.attrs["axis"].lower(): name  for ( name, coord ) in variable.coords.items() }
        except:
            return { cls._getCoordName( index, len(variable.dims) ): variable.dims[index] for index in range( len(variable.dims) ) }

    @classmethod
    def getAxisMap( cls, variable: xr.DataArray ) -> Dict[str,str]:
        try:
            return { name: coord.attrs["axis"].lower()  for ( name, coord ) in variable.coords.items() }
        except:
            return { variable.dims[index]: cls._getCoordName( index, len(variable.dims) ) for index in range( len(variable.dims) ) }

    def hasAxis( self, axis: str ) -> bool:
        return self.axes.count( axis ) > 0

    def mapCoordinates(self, variable: xr.DataArray ) -> xr.DataArray:
        axis_map = self.getAxisMap( variable )
        return variable.rename( axis_map )

    def getMappedVariables(self, dataset: xr.Dataset ) -> List[xr.DataArray]:
        return [ self.mapCoordinates( dataset[varName] ) for varName in self.varNames() ]



