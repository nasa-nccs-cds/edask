from abc import ABCMeta, abstractmethod
import logging, cdms2, time, os, itertools, random, string
from edask.messageParser import mParse
from typing import List, Dict, Sequence, BinaryIO, TextIO, ValuesView, Tuple
import xarray as xr

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

class KernelResult:

    def __init__( self, _domain: str, _dataset: xr.Dataset = None, ids: List[str] = [] ):
        self.dataset: xr.Dataset = _dataset
        self._ids = []
        self.domain = _domain
        self.logger = logging.getLogger()
        self.addIds( ids )

    @property
    def ids(self): return self._ids
    def addIds(self, ids: List[str]): self._ids.extend( ids )

    @staticmethod
    def empty( domain: str ) -> "KernelResult": return KernelResult( domain )
    def initDatasetList(self) -> List[xr.Dataset]: return [] if self.dataset is None else [self.dataset]
    def getInputs(self) -> List[xr.DataArray]: return [ self.dataset[vid] for vid in self.ids ]
    def getVariables(self) -> List[xr.DataArray]:
        self.logger.info( "GetVariables[ ids: {} ]( vars = {} )".format( str(self.ids), str( list(self.dataset.variables.keys()) ) ) )
        return self.getInputs()

    def getLargestVariable(self) -> xr.DataArray:
        largest: xr.DataArray = None
        for vid in self.ids:
            var: xr.DataArray = self.dataset[vid]
            if largest is None or var.size > largest.size: largest = var
        return largest

    def getSmallestVariable(self) -> xr.DataArray:
        smallest: xr.DataArray = None
        for vid in self.ids:
            var: xr.DataArray = self.dataset[vid]
            if smallest is None or var.size < smallest.size: smallest = var
        return smallest

    def getAlignmentVariable(self, alignRes: str ):
        if alignRes.startswith("low"): return self.getSmallestVariable()
        if alignRes.startswith("high"): return self.getLargestVariable()
        else: raise Exception( "Unrecognized alignment resolution parameter (should be 'high' or 'low'): " + alignRes )

    def align( self, alignRes: str = "lowest" ) -> "KernelResult":
      if not alignRes: return self
      target_var =  self.getAlignmentVariable( alignRes )
      new_vars = []
      for var in self.getVariables():
          if (var.shape == target_var.shape) and (var.dims == target_var.dims):
              new_vars.append( var )
          else:
              new_vars.append( var.interp_like( target_var, assume_sorted=True ) )
      dataset = xr.Dataset( { array.name: array for array in new_vars }  )
      return KernelResult( self.domain, dataset, [ array.name for array in new_vars ] )

    def addResult(self, new_dataset: xr.Dataset, new_ids: List[str] ):
        self.dataset = new_dataset if self.dataset is None else xr.merge( [self.dataset, new_dataset] )
        self.addIds( new_ids )

    def addArray(self, array: xr.DataArray, attrs ):
        self.logger.info( "AddArray( var = {} )".format( str(array.name) ) )
        if self.dataset is None: self.dataset = xr.Dataset( { array.name: array, }  )
        else: self.dataset.merge_data_and_coords( array )
        self.addIds( [ array.name ] )

    @staticmethod
    def merge( kresults: List["KernelResult"] ):
        merged_dataset = xr.merge( [ kr.dataset for kr in kresults ] )
        merged_ids = "-".join( itertools.chain( *[ kr.ids for kr in kresults ] ) )
        return KernelResult( merged_ids, merged_dataset )
