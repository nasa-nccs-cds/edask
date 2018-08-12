from typing import  List, Dict, Any, Sequence, Union, Optional
from enum import Enum

class Axis(Enum):
    UNKNOWN = 0
    X = 1
    Y = 2
    Z = 3
    T = 4

    @classmethod
    def parse(cls, name: str):
        n = name.lower()
        if n.startswith("x") or n.startswith("lat"): return cls.X
        if n.startswith("y") or n.startswith("lon"): return cls.Y
        if n.startswith("z") or n.startswith("lev") or n.startswith("plev"): return cls.Z
        if n.startswith("t") or n.startswith("time"): return cls.T
        return cls.UNKNOWN

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

class Domain:

    @classmethod
    def new(cls, domainSpec: Dict[str, Any] ):
        name = "d0"
        axisBounds = {}
        for ( key, value ) in domainSpec.items():
            if( key.lower() in [ "name, :id"]):
                name = value
            else:
                axisBounds[ key ] = AxisBounds.new( key, value )
        return Domain( name,  axisBounds )

    def __init__( self, _name: str, _axisBounds: List[AxisBounds] ):
        self.name = _name
        self.axisBounds = _axisBounds

    def findAxisBounds( self, type: Axis ) -> Optional[AxisBounds]:
        for axis in self.axisBounds:
            if axis.type == type: return axis
        return None

    def hasUnknownAxes(self) -> bool :
        return self.findAxisBounds(Axis.UNKNOWN) is not None


class DomainManager:

    @classmethod
    def new(cls, domainSpecs: List[Dict[str, Any]] ):
        domains = [ Domain.new(domainSpec) for domainSpec in domainSpecs ]
        return DomainManager( { d.name.lower(): d for d in domains } )

    def __init__(self, _domains: Dict[str,Domain] ):
        self.domains = _domains

    def getDomain( self, name: str ) -> Domain:
        return self.domains.get( name.lower() )






