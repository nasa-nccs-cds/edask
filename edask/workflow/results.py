from abc import ABCMeta, abstractmethod
import logging, cdms2, time, os, itertools, random, string
from edask.messageParser import mParse
from collections import OrderedDict
from enum import Enum, auto
from typing import List, Dict, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
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

    def aligned( self, other: "EDASArray" ):
        return ( self.domId == other.domId ) and ( self.data.shape == other.data.shape ) and ( self.data.dims == other.data.dims )

    def align( self, other: "EDASArray", assume_sorted=True ):
        assert self.domId == other.domId, "Cannot align variable with different domains: {} vs {}".format( self.data.name, other.data.name, )
        if self.aligned( other ): return self
        new_data = self.data.interp_like( other.data, assume_sorted )
        return EDASArray( self.domId, new_data )

class EDASDataset:

    def __init__( self,  _dataset: Optional[xr.Dataset], varList: Dict[str,str] ):
        self.dataset: xr.Dataset = _dataset
        self._ids = []
        self._varList: Dict[str,str] = OrderedDict()
        self.logger = logging.getLogger()
        self.addVars( varList )

    @staticmethod
    def new( edasArrays: List[EDASArray] ):
        varList: Dict[str,str] = {}
        dataset = xr.Dataset( { a.data.name:a.data for a in edasArrays } )
        varList: Dict[str,str] = { a.data.name:a.domId for a in edasArrays }
        return EDASDataset( dataset, varList )

    @property
    def ids(self): return self._varList.keys()
    def addVars(self, varList: Dict[str,str] ): self._varList.update( varList )

    @staticmethod
    def empty() -> "EDASDataset": return EDASDataset(None, {})
    def initDatasetList(self) -> List[xr.Dataset]: return [] if self.dataset is None else [self.dataset]
    def getInputs(self) -> List[EDASArray]: return [ EDASArray( domId, self.dataset[vid] ) for ( vid, domId ) in self._varList.items() ]
    def getVariables(self) -> List[EDASArray]:
        self.logger.info( "GetVariables[ ids: {} ]( vars = {} )".format( str(self.ids), str( list(self.dataset.variables.keys()) ) ) )
        return self.getInputs()

    def getExtremeVariable(self, ext: Extremity ) -> EDASArray:
        inputs = self.getInputs()
        sizes = [ x.size for x in inputs ]
        exVal = max( sizes ) if ext == Extremity.HIGHEST else min( sizes )
        return inputs[ sizes.index( exVal ) ]

    def getAlignmentVariable(self, alignRes: str ):
        return self.getExtremeVariable( Extremity.parse(alignRes) )

    def align( self, alignRes: str = "lowest" ) -> "EDASDataset":
      if not alignRes: return self
      target_var: EDASArray =  self.getAlignmentVariable( alignRes )
      new_vars: List[EDASArray] = [ var.align(target_var) for var in self.getVariables() ]
      return EDASDataset.new( new_vars )

    def addResult(self, new_dataset: xr.Dataset, varList: Dict[str,str] ):
        self.dataset = new_dataset if self.dataset is None else xr.merge( [self.dataset, new_dataset] )
        self.addVars( varList )

    def addArray(self, domId: str, array: xr.DataArray, attrs ):
        self.logger.info( "AddArray( var = {} )".format( str(array.name) ) )
        if self.dataset is None: self.dataset = xr.Dataset( { array.name: array, }  )
        else: self.dataset.merge_data_and_coords( array )
        self.addVars( { array.name: domId } )

    @staticmethod
    def merge(kresults: List["EDASDataset"]):
        merged_dataset = xr.merge( [ kr.dataset for kr in kresults ] )
        merged_ids = "-".join( itertools.chain( *[ kr.ids for kr in kresults ] ) )
        return EDASDataset(merged_ids, merged_dataset)
