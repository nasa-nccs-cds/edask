from typing import  List, Dict, Any, Sequence, Union, Optional, Tuple
from enum import Enum, auto
import xarray as xr

class Axis(Enum):
    UNKNOWN = auto()
    X = auto()
    Y = auto()
    Z = auto()
    T = auto()

    @classmethod
    def parse(cls, name: str) -> "Axis":
        n = name.lower()
        if n.startswith("x") or n.startswith("lon"): return cls.X
        if n.startswith("y") or n.startswith("lat"): return cls.Y
        if n.startswith("z") or n.startswith("lev") or n.startswith("plev"): return cls.Z
        if n.startswith("t") or n.startswith("time"): return cls.T
        return cls.UNKNOWN

    @classmethod
    def bestGuess(cls, variable: xr.DataArray, index: int) -> "Axis":
        firstGuess = cls.parse( variable.name )
        if firstGuess == cls.UNKNOWN:
            if "time" in variable.dims:
                if   len(variable.dims) == 1: return cls.T
                elif len(variable.dims) == 2: return [ cls.T, cls.Z ][index]
                elif len(variable.dims) == 3: return [ cls.T, cls.Y, cls.X ][index]
                elif len(variable.dims) == 4: return [ cls.T, cls.Z, cls.Y, cls.X ][index]
            else:
                if   len(variable.dims) == 1: return cls.Z
                elif len(variable.dims) == 2: return [ cls.Y, cls.X ][index]
                elif len(variable.dims) == 3: return [ cls.Z, cls.Y, cls.X ][index]
        else: return firstGuess

    @classmethod
    def getCoordMap( cls, variable: xr.DataArray ) -> Dict["Axis",str]:
        try:
            return { cls.parse(coord.attrs["axis"]): name  for ( name, coord ) in variable.coords.items() }
        except:
            return { cls.bestGuess(variable, index): variable.dims[index] for index in range(len(variable.dims)) }

    @classmethod
    def getAxisMap( cls, variable: xr.DataArray ) -> Dict[str,"Axis"]:
        try:
            return { name: cls.parse(coord.attrs["axis"])  for ( name, coord ) in variable.coords.items() }
        except:
            return { variable.dims[index]: cls.bestGuess(variable, index) for index in range(len(variable.dims)) }

    @classmethod
    def getAxisAttr( cls, coord: xr.DataArray ) -> "Axis":
        if "axis" in coord.attrs: return cls.parse( coord.attrs["axis"] )
        else: return cls.UNKNOWN

    @classmethod
    def updateMap( cls, axis_map: Dict, name: str, axis: "Axis", nameToAxis: bool, axis2str: bool ):
        aval = axis.name.lower() if axis2str else axis
        if nameToAxis:  axis_map[name] = aval
        else:           axis_map[aval] = name

    @classmethod
    def getDatasetCoordMap( cls, dset: xr.Dataset, nameToAxis = True, axis2str = True ) -> Dict:
        axis_map = {}
        for ( name, coord ) in dset.coords.items():
            axis = cls.getAxisAttr( coord )
            if axis == cls.UNKNOWN: axis = cls.parse(name)
            if axis != cls.UNKNOWN: cls.updateMap( axis_map, name, axis, nameToAxis, axis2str )
        return axis_map

class AxisBounds:

    @classmethod
    def new(cls, name, boundsSpec ):
        if isinstance( boundsSpec, dict ):
            start = boundsSpec.get("start",None)
            end = boundsSpec.get("end",None)
            system = boundsSpec.get("system",None)
            return AxisBounds( name, start, end, system, boundsSpec )
        else:
            value = boundsSpec
            return AxisBounds(name, value, value, "values", boundsSpec)


    def __init__(self, _name: str, _start: Union[float,int,str], _end: Union[float,int,str], _system: str, _metadata: Dict ):
        self.name = _name
        self.type = Axis.parse( _name )
        self.start = _start
        self.end = _end
        self.system = _system
        self.metadata = _metadata

    def slice(self) -> slice:
        return slice( self.start, self.end )

    def __str__(self):
        return "B({}:{})[ start: {}, end: {}, system: {} ]".format( self.type.name, self.name, self.start, self.end, self.system )

class Domain:

    @classmethod
    def new(cls, domainSpec: Dict[str, Any] ):
        name = "d0"
        axisBounds: Dict[Axis,AxisBounds] = {}
        for ( key, value ) in domainSpec.items():
            if( key.lower() in [ "name", "id" ] ):
                name = value
            else:
                bounds = AxisBounds.new( key, value )
                axisBounds[ bounds.type ] = bounds
        return Domain( name,  axisBounds )

    def __init__( self, _name: str, _axisBounds: Dict[Axis,AxisBounds] ):
        self.name = _name
        self.axisBounds: Dict[Axis,AxisBounds] = _axisBounds

    def findAxisBounds( self, type: Axis ) -> Optional[AxisBounds]:
        return self.axisBounds.get( type, None )

    def hasUnknownAxes(self) -> bool :
        return self.findAxisBounds(Axis.UNKNOWN) is not None

    def rename(self, nameMap: Dict[str,str] ) -> "Domain":
        bounds = []
        for ( axis, bound ) in self.axisBounds.items():
            if ( axis == Axis.UNKNOWN ) and ( bound.name in nameMap):
                bounds.append(( Axis.parse(nameMap[bound.name]), bound))
            else: bounds.append( (axis, bound) )
        return Domain( self.name, dict( bounds ) )

    @classmethod
    def slice( cls, axis: Axis, bounds: AxisBounds ) -> Tuple[str,slice]:
         return ( bounds.name if axis == Axis.UNKNOWN else axis.name.lower(), bounds.slice() )

    def subset( self, dset: xr.Dataset ) -> xr.Dataset:
        for system in [ "val", "ind" ] :
            bounds_list = [ self.slice( axis, bounds ) for (axis, bounds) in self.axisBounds.items() if bounds.system.startswith( system ) ]
            if( len(bounds_list) ): dset = dset.sel( dict( bounds_list ) ) if system == "val" else dset.isel( dict( bounds_list ) )
        return dset

    def __str__(self):
        return "D({})[ {} ]".format( self.name, "; ".join( [ str(b) for b in self.axisBounds.values()] ) )


class DomainManager:

    @classmethod
    def new(cls, domainSpecs: List[Dict[str, Any]] ):
        domains = [ Domain.new(domainSpec) for domainSpec in domainSpecs ]
        return DomainManager( { d.name.lower(): d for d in domains } )

    def __init__(self, _domains: Dict[str,Domain] ):
        self.domains = _domains

    def getDomain( self, name: str ) -> Domain:
        return self.domains.get( name.lower() )

    def __str__(self):
        return "Domains[ {} ]".format( "; ".join( [ str(d) for d in self.domains.values() ] )  )






