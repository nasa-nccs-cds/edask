from abc import ABCMeta, abstractmethod
import logging, cdms2, time, os, itertools, random, string, copy
from edask.messageParser import mParse
from collections import OrderedDict
from enum import Enum, auto
from typing import List, Dict, Sequence, BinaryIO, Any, Callable, Tuple, Optional, Set
from edask.process.domain import Domain, Axis
import xarray as xr

class Extremity(Enum):
    HIGHEST = auto()
    LOWEST = auto()

    @classmethod
    def parse(cls, name: str) -> "Extremity":
        n = name.lower()
        if n.startswith("high") or n.startswith("large") or n.startswith("min"): return cls.HIGHEST
        if n.startswith("low") or n.startswith("small") or n.startswith("max"): return cls.LOWEST
        else: raise Exception( "Unrecognized parameter value: " + name )

class KernelSpec:
    def __init__( self, name, title, description, **kwargs ):
        self._name = name
        self._title = title
        self._description = description
        self._options = kwargs

    @property
    def name(self): return self._name

    @property
    def description(self): return self._description

    @property
    def title(self): return self._title

    @property
    def summary(self) -> str: return ";".join( [ self._name, self.title ] )

    def __str__(self): return ";".join( [ self._name, self.title, self.description, str(self._options) ] )

class EDASArray:
    def __init__( self, _domId: str, _data: xr.DataArray ):
        self.domId = _domId
        self.data = _data

    @property
    def size(self) -> int: return self.data.size

    @property
    def name(self) -> str: return self.data.name

    @name.setter
    def name(self, value): self.data.name = value

    def xrDataset(self, attrs: Dict[str,Any] = None) -> xr.Dataset:
        return xr.Dataset( { self.data.name: self.data }, attrs=attrs )

    def aligned( self, other: "EDASArray" ):
        return ( self.domId == other.domId ) and ( self.data.shape == other.data.shape ) and ( self.data.dims == other.data.dims )

    def align( self, other: "EDASArray", assume_sorted=True ):
        assert self.domId == other.domId, "Cannot align variable with different domains: {} vs {}".format( self.data.name, other.data.name, )
        if self.aligned( other ): return self
        new_data = self.data.interp_like( other.data, "linear", assume_sorted )
        return EDASArray( self.domId, new_data )

    def updateData(self, new_data: xr.DataArray ) -> "EDASArray":
        return EDASArray( self.domId, new_data )

    def subset( self, domain: Domain ) -> "EDASArray":
        xarray = self.data
        for system in [ "val", "ind" ]:
            bounds_list = [ domain.slice( axis, bounds ) for (axis, bounds) in domain.axisBounds.items() if bounds.system.startswith( system ) ]
            if( len(bounds_list) ): xarray = xarray.sel( dict( bounds_list ) ) if system == "val" else xarray.isel( dict( bounds_list ) )
        return EDASArray( self.domId, xarray )

    # def update(self, op: Callable[[xr.DataArray,Any],xr.DataArray], **kwargs ) -> "EDASArray":
    #     return EDASArray( self.domId, op(self.data, **kwargs) )

    @staticmethod
    def domains( inputs: List["EDASArray"] ) -> Set[str]:
        return { var.domId for var in inputs }

    def axis(self, axis: Axis ):
        return self.data.coords.get( axis.name.lower() )

    def axes(self) -> List[str]:
        return self.data.coords.keys()

    def max( self, axes: List[str] ) -> "EDASArray":
        return EDASArray( self.domId, self.data.max( dim=axes, keep_attrs=True ) )

    def min( self, axes: List[str] ) -> "EDASArray":
        return EDASArray( self.domId, self.data.min( dim=axes, keep_attrs=True) )

    def mean( self, axes: List[str] ) -> "EDASArray":
        return EDASArray( self.domId, self.data.mean( dim=axes, keep_attrs=True) )

    def median( self, axes: List[str] ) -> "EDASArray":
        return EDASArray( self.domId, self.data.median( dim=axes, keep_attrs=True) )

    def var( self, axes: List[str] ) -> "EDASArray":
        return EDASArray( self.domId, self.data.var( dim=axes, keep_attrs=True) )

    def std( self, axes: List[str] ) -> "EDASArray":
        return EDASArray( self.domId, self.data.std( dim=axes, keep_attrs=True) )

    def sum( self, axes: List[str] ) -> "EDASArray":
        return EDASArray( self.domId, self.data.sum( dim=axes, keep_attrs=True) )

    def __sub__(self, other: "EDASArray") -> "EDASArray":
        assert self.domId == other.domId, "Can't combine arrays with different domains"
        rv = self.data - other.data
        return EDASArray( self.domId, rv )

    def __add__(self, other: "EDASArray") -> "EDASArray":
        assert self.domId == other.domId, "Can't combine arrays with different domains"
        return EDASArray( self.domId, self.data + other.data )

    def __mul__(self, other: "EDASArray") -> "EDASArray":
        assert self.domId == other.domId, "Can't combine arrays with different domains"
        return EDASArray( self.domId, self.data * other.data )

    def __truediv__(self, other: "EDASArray") -> "EDASArray":
        assert self.domId == other.domId, "Can't combine arrays with different domains"
        return EDASArray( self.domId, self.data / other.data )

class EDASDataset:

    def __init__( self, _arrayMap: Dict[str,EDASArray], _attrs: Dict[str,Any]  ):
        self.arrayMap: Dict[str,EDASArray] = _arrayMap
        self.attrs = _attrs
        self.logger = logging.getLogger()

    @staticmethod
    def init( arrays: List[EDASArray], attrs: Dict[str,Any]  ):
        dataset = EDASDataset.empty()
        return dataset.addArrays(arrays,attrs)

    @staticmethod
    def new( dataset: xr.Dataset, varMap: Dict[str,str] = {}, idMap: Dict[str,str] = {} ):
        dataset.rename(idMap, True)
        arrayMap = { vid: EDASArray( domId, dataset[vid] ) for ( vid, domId ) in varMap.items() }
        return EDASDataset( arrayMap, dataset.attrs )

    def addArrays(self, arrays: List[EDASArray], attrs: Dict[str,Any]  ) -> "EDASDataset":
        for array in arrays: self.arrayMap[array.name] = array
        self.attrs.update(attrs)
        return self

    def addArray(self, array: EDASArray, attrs: Dict[str,Any]  ) -> "EDASDataset":
        self.arrayMap[array.name] = array
        self.attrs.update(attrs)
        return self

    @property
    def domains(self) -> Set[str]: return { array.domId for array in self.arrayMap.values() }

    @property
    def ids(self) -> Set[str]: return set( self.arrayMap.keys() )

    @property
    def id(self) -> str: return "-".join( self.arrayMap.keys() )

    @property
    def vars2doms(self) -> Dict[str,str]: return { name:array.domId for ( name, array ) in self.arrayMap.items() }

    @staticmethod
    def empty() -> "EDASDataset": return EDASDataset( {}, {} )

    @staticmethod
    def domainSet( inputs: List["EDASDataset"], opDomains: Set[str] = None ) -> Set[str]:
        rv = set()
        for dset in inputs: rv = rv.union( dset.domains )
        return rv if opDomains is None else rv | opDomains

    @staticmethod
    def mergeVarMaps( inputs: List["EDASDataset"] ) -> Dict[str,str]:
        rv = {}
        for dset in inputs: rv = rv.update( dset.vars2doms )
        return rv

    @property
    def inputs(self) -> List[EDASArray]: return list(self.arrayMap.values())

    def subset( self, domain: Domain ):
        arrayMap = { vid: array.subset( domain ) for ( vid, array ) in self.arrayMap.items() }
        return EDASDataset( arrayMap, self.attrs )

    def requiresSubset(self, target_domain: str ) -> bool:
        return len( self.domains.difference( { target_domain } ) ) > 0

    def getExtremeVariable(self, ext: Extremity ) -> EDASArray:
        sizes = [ x.size for x in self.inputs ]
        exVal = max( sizes ) if ext == Extremity.HIGHEST else min( sizes )
        return self.inputs[ sizes.index( exVal ) ]

    def getAlignmentVariable(self, alignRes: str ):
        return self.getExtremeVariable( Extremity.parse(alignRes) )

    def align( self, alignRes: str = "lowest" ) -> "EDASDataset":
      if not alignRes: return self
      target_var: EDASArray =  self.getAlignmentVariable( alignRes )
      new_vars: List[EDASArray] = [ var.align(target_var) for var in self.inputs ]
      return EDASDataset.init( new_vars, self.attrs )

    def addDataset(self, dataset: xr.Dataset, varMap: Dict[str,str] ):
        arrays = [ EDASArray( domId, dataset[vid] ) for ( vid, domId ) in varMap.items() ]
        self.addArrays( arrays, dataset.attrs )

    def __iadd__(self, other: "EDASDataset" ) -> "EDASDataset":
        self.arrayMap.update( other.arrayMap )
        self.attrs.update( other.attrs )
        return self

    @staticmethod
    def merge(dsets: List["EDASDataset"]):
        if len( dsets ) == 1: return dsets[0]
        arrayMap: Dict[str,EDASArray] = {}
        attrs: Dict[str,Any] = {}
        for dset in dsets:
            arrayMap.update( dset.arrayMap )
            attrs.update( dset.attrs )
        return EDASDataset( arrayMap, attrs )
