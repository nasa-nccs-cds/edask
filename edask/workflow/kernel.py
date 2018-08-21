from abc import ABCMeta, abstractmethod
import logging, cdms2, time, os, itertools, random, string
from edask.messageParser import mParse
from edask.process.task import TaskRequest
from typing import List, Dict, Sequence, BinaryIO, TextIO, ValuesView, Tuple
from edask.process.operation import WorkflowNode, SourceNode, OpNode
import xarray as xr
from .results import KernelSpec, KernelResult
from edask.process.source import SourceType
from edask.process.source import VariableSource, DataSource
from edask.process.domain import Axis
from edask.agg import Collection

class Kernel:

    __metaclass__ = ABCMeta

    def __init__( self, spec: KernelSpec ):
        self.logger = logging.getLogger()
        self._spec: KernelSpec = spec
        self._id: str  = self._spec.name + "-" + ''.join([ random.choice( string.ascii_letters + string.digits ) for n in range(5) ] )

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
        inputVars: KernelResult = self.preprocessInputs( request, op, inputs )
        result: KernelResult = KernelResult.empty( inputVars.domain )
        for variable in inputVars.getVariables():
            resultArray: xr.DataArray = self.processVariable( request, op, variable )
            resultArray.name = op.getResultId( variable.name )
            self.logger.info( " Process Input {} -> {}".format( variable.name, resultArray.name ) )
            result.addArray( resultArray, inputVars.dataset.attrs )
        return result

    @abstractmethod
    def processVariable( self, request: TaskRequest, node: OpNode, inputs: xr.DataArray ) -> xr.DataArray: pass

    def preprocessInputs( self, request: TaskRequest, op: OpNode, inputs: List[KernelResult] ) -> KernelResult:
        merged_domain: str  = request.intersectDomains( { op.domain } | { kr.domain for kr in inputs } )
        result: KernelResult = KernelResult.empty( merged_domain )
        kernelInputs = [ request.subsetResult( merged_domain, input ) for input in inputs ]
        for kernelInput in kernelInputs:
            for variable in kernelInput.getVariables():
                result.addArray( variable, kernelInput.ids )
        return result.align()

class InputKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("input", "Data Input","Data input and workflow source node" ) )

    def buildWorkflow( self, request: TaskRequest, node: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        snode: SourceNode = node
        dataSource: DataSource = snode.varSource.dataSource
        result: KernelResult = KernelResult.empty(snode.domain)
        if dataSource.type == SourceType.collection:
            collection = Collection.new( dataSource.address )
            aggs = collection.sortVarsByAgg( snode.varSource.vids )
            for ( aggId, vars ) in aggs.items():
                dset = xr.open_mfdataset( collection.pathList(aggId), autoclose=True, data_vars=vars, parallel=True)
                result.addResult( *self.processDataset( request, dset, snode )  )
        elif dataSource.type == SourceType.file:
            self.logger.info( "Reading data from address: " + dataSource.address )
            dset = xr.open_mfdataset(dataSource.address, autoclose=True, data_vars=snode.varSource.ids(), parallel=True)
            result.addResult( *self.processDataset( request, dset, snode ) )
        elif dataSource.type == SourceType.dap:
            dset = xr.open_dataset( dataSource.address, autoclose=True  )
            result.addResult( *self.processDataset( request, dset, snode ) )
        return result

    def processDataset(self, request: TaskRequest, dset: xr.Dataset, snode: SourceNode ) -> ( xr.Dataset, List[str] ):
        coordMap = Axis.getDatasetCoordMap( dset )
        idMap: Dict[str,str] = snode.varSource.name2id(coordMap)
        dset.rename( idMap, True )
        roi = snode.domain  #  .rename( idMap )
        return ( request.subset( roi, dset ), snode.varSource.ids() )