from typing import  List, Dict, Any, Sequence, Union, Optional, Tuple, Set
import string, random
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
        if isinstance( _start, str ): assert  isinstance( _end, str ), "Axis {}: Start & end bounds must have same encoding: start={}, end={}".format( self.name, self.start, self.end)
        else: assert  _end >= _start, "Axis {}: Start bound cannot be greater then end bound: start={}, end={}".format( self.name, self.start, self.end)
        self.type = Axis.parse( _name )
        self.system = _system
        self.start = _start
        self.end = _end + 1 if _system.startswith("ind") else _end
        self.metadata = _metadata

    def canBroadcast(self) -> bool:
        return self.start == self.end

    def slice(self) -> slice:
        return slice( self.start, self.end )

    def intersect(self, other: "AxisBounds", allow_broadcast: bool = True ) -> "AxisBounds":
        if other is None: return None if (allow_broadcast and self.canBroadcast()) else self
        assert self.system ==  other.system, "Can't intersect domain axes with different systems: Axis {}, {} vs {}".format( self.name, self.system, other.system )
        if allow_broadcast and other.canBroadcast(): return self
        if allow_broadcast and self.canBroadcast(): return other
        if isinstance( self.start, str ):      # TODO: convert time strings to date reps
            new_start = max( self.start, other.start )
            new_end = min( self.end, other.end )
            return AxisBounds( self.name, new_start, new_end, self.system, self.metadata )
        else:
            new_start = max( self.start, other.start )
            new_end = min( self.end, other.end )
            return AxisBounds( self.name, new_start, new_end, self.system, self.metadata )

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

    def intersect( self, name: str, other: "Domain", allow_broadcast: bool = True ) -> "Domain":
        result_axes: Dict[Axis,AxisBounds]  = {}
        other_axes = dict( other.axisBounds )
        for (axis,bounds) in self.axisBounds:
            intersected_bounds = bounds.intersect( other_axes.pop( axis ), allow_broadcast )
            if intersected_bounds: result_axes[axis] = intersected_bounds
        for (axis,bounds) in other_axes:
            if not (allow_broadcast and bounds.canBroadcast() ):
                result_axes[axis] = bounds
        return Domain( name, result_axes )

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

    def subset( self, array: xr.DataArray ) -> xr.DataArray:
        for system in [ "val", "ind" ] :
            bounds_list = [ self.slice( axis, bounds ) for (axis, bounds) in self.axisBounds.items() if bounds.system.startswith( system ) ]
            if( len(bounds_list) ): array = array.sel( dict( bounds_list ) ) if system == "val" else array.isel( dict( bounds_list ) )
        return array

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

    def intersectDomains(self, domainIds: Set[str], allow_broadcast: bool = True ) -> Optional[str]:
        if len( domainIds ) == 0: return None
        if len( domainIds ) == 1: return domainIds.pop()
        new_domId = self.randomId(4)
        domains: List[Domain] = [ self.getDomain(id) for id in domainIds ]
        result_domain: Domain = domains[0]
        for domain in domains[1:]:
            result_domain = result_domain.intersect( new_domId, domain, allow_broadcast )
        self.domains[ new_domId ] = result_domain
        return new_domId

    def randomId(self, length )-> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join( random.SystemRandom().choice( tokens ) for _ in range( length ) )

    def __str__(self):
        return "Domains[ {} ]".format( "; ".join( [ str(d) for d in self.domains.values() ] )  )






