from abc import ABCMeta, abstractmethod
import logging, cdms2, time, os, itertools, random, string
from edask.messageParser import mParse
from edask.process.task import TaskRequest
from typing import List, Dict, Sequence, BinaryIO, TextIO, ValuesView, Tuple
from edask.process.operation import WorkflowNode, SourceNode, OpNode
import xarray as xr
from .results import KernelSpec, KernelResult

class Kernel:

    __metaclass__ = ABCMeta

    def __init__( self, spec: KernelSpec ):
        self.logger = logging.getLogger()
        self._spec: KernelSpec = spec
        self._id: str  = ''.join([ random.choice( string.ascii_letters + string.digits ) for n in range(6) ])

    def name(self): return self._spec.name

    def getSpec(self) -> KernelSpec: return self._spec
    def getCapabilities(self) -> str: return self._spec.summary
    def describeProcess( self ) -> str: return str(self._spec)

    def getResultDataset(self, request: TaskRequest, node: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        result = request.getCachedResult( self._id )
        if result is None:
           result = self.buildWorkflow( request, node, inputs )
           request.cacheResult( self._id, result )
        return result

    @abstractmethod
    def buildWorkflow( self, request: TaskRequest, node: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult: pass

class OpKernel(Kernel):

    def buildWorkflow( self, request: TaskRequest, wnode: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( [ str(w) for w in op.inputs ] ) + ", op metadata = " + str(op.metadata) + ", axes = " + str(op.axes) )
        result: KernelResult = KernelResult.empty()
        inputVars: List[Tuple[xr.DataArray,xr.Dataset]] = self.preprocessInputs( request, op, [ ( var, kernelResult.dataset ) for kernelResult in inputs for var in kernelResult.getInputs() ] )
        for ( variable, dataset ) in inputVars:
            resultArray: xr.DataArray = self.processVariable( request, op, variable )
            resultArray.name = op.getResultId( variable.name )
            self.logger.info(" Process Input {} -> {}".format( variable.name, resultArray.name ))
            result.addArray( resultArray, dataset.attrs )
        return result

    @abstractmethod
    def processVariable( self, request: TaskRequest, node: OpNode, inputs: xr.DataArray ) -> xr.DataArray: pass

    def preprocessInputs( self, request: TaskRequest, op: OpNode, inputs: List[Tuple[xr.DataArray,xr.Dataset]] ) -> List[Tuple[xr.DataArray,xr.Dataset]]:
        return inputs
