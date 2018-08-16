from abc import ABCMeta, abstractmethod
import logging, cdms2, time, os, itertools
from edask.messageParser import mParse
from edask.process.task import TaskRequest
from typing import List, Dict, Sequence, BinaryIO, TextIO, ValuesView
from edask.process.operation import WorkflowNode
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

    def __init__( self, dataset: xr.Dataset = None, ids: List[str] = [] ):
        self.dataset: xr.Dataset = dataset
        self._ids = []
        self.logger = logging.getLogger()
        self.addIds( ids )

    @property
    def ids(self): return self._ids
    def addIds(self, ids: List[str]): self._ids.extend( ids )

    @staticmethod
    def empty() -> "KernelResult": return KernelResult()
    def initDatasetList(self) -> List[xr.Dataset]: return [] if self.dataset is None else [self.dataset]
    def getInputs(self) -> List[xr.DataArray]: return [ self.dataset[vid] for vid in self.ids ]
    def getVariables(self) -> List[xr.DataArray]:
        self.logger.info( "GetVariables[ ids: {} ]( vars = {} )".format( str(self.ids), str( list(self.dataset.variables.keys()) ) ) )
        return self.getInputs()

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
        merged_ids = list( itertools.chain( *[ kr.ids for kr in kresults ] ) )
        return KernelResult( merged_dataset, merged_ids )

class Kernel:

    __metaclass__ = ABCMeta

    def __init__( self, spec: KernelSpec ):
        self.logger = logging.getLogger()
        self._spec: KernelSpec = spec
        self._cachedresult: KernelResult = None

    def name(self): return self._spec.name

    def getSpec(self) -> KernelSpec: return self._spec
    def getCapabilities(self) -> str: return self._spec.summary
    def describeProcess( self ) -> str: return str(self._spec)
    def clear(self): self._cachedresult = None

    def getResultDataset(self, request: TaskRequest, node: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        if self._cachedresult is None:
           self._cachedresult = self.buildWorkflow( request, node, inputs )
        self.logger.info( " GetResultDataset[{}]: ids= {}, vars= {} ".format( self._spec.name, str( self._cachedresult.ids ), str( list( self._cachedresult.dataset.variables.keys()) ) ))
        return self._cachedresult

    @abstractmethod
    def buildWorkflow( self, request: TaskRequest, node: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult: pass
