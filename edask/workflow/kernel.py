from abc import ABCMeta, abstractmethod
import logging, random, string
from edask.process.task import TaskRequest
from typing import List, Dict, Set, Any, Optional
from edask.process.operation import WorkflowNode, SourceNode, OpNode
import xarray as xr
from .results import KernelSpec, EDASDataset, EDASArray
from edask.process.source import SourceType
from edask.process.source import DataSource
from edask.process.domain import Axis
from edask.collections.agg import Collection

class Kernel:

    __metaclass__ = ABCMeta

    def __init__( self, spec: KernelSpec ):
        self.logger = logging.getLogger()
        self._spec: KernelSpec = spec
        self._id: str  = self._spec.name + "-" + ''.join([ random.choice( string.ascii_letters + string.digits ) for n in range(5) ] )

    @property
    def name(self): return self._spec.name

    def getSpec(self) -> KernelSpec: return self._spec
    def getCapabilities(self) -> str: return self._spec.summary
    def describeProcess( self ) -> str: return str(self._spec)

    def getResultDataset(self, request: TaskRequest, node: WorkflowNode, inputs: List[EDASDataset]) -> EDASDataset:
        result = request.getCachedResult( self._id )
        if result is None:
           result = self.buildWorkflow( request, node, inputs )
           request.cacheResult( self._id, result )
        return result

    @abstractmethod
    def buildWorkflow(self, request: TaskRequest, node: WorkflowNode, inputs: List[EDASDataset]) -> EDASDataset: pass

class OpKernel(Kernel):
    # Operates independently on sets of variables with same index across all input datasets
    # Will independently pre-subset to intersected domain and pre-align all variables in each set if necessary.

    def buildWorkflow(self, request: TaskRequest, wnode: WorkflowNode, inputs: List[EDASDataset]) -> EDASDataset:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( [ str(w) for w in op.inputs ] ) + ", op metadata = " + str(op.metadata) + ", axes = " + str(op.axes) )
        result: EDASDataset = EDASDataset.empty()
        input_vars: List[List[EDASArray]] = [ dset.inputs for dset in inputs ]
        matched_inputs: List[EDASArray] = None
        for matched_inputs in zip( *input_vars ):
            inputVars: EDASDataset = self.preprocessInputs(request, op, matched_inputs, inputs[0].attrs )
            inputCrossSection: EDASDataset = self.mergeEnsembles(request, op, inputVars)
            product = self.processInputCrossSection( request, op, inputCrossSection )
            product.name = op.getResultId( inputVars.id )
            result += product
        return result

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset ) -> EDASDataset:
        results: List[EDASArray] = [ self.processVariable( request, node, input ) for input in inputDset.inputs ]
        return EDASDataset.init( results, inputDset.attrs )

    @abstractmethod
    def processVariable( self, request: TaskRequest, node: OpNode, inputs: EDASArray ) -> EDASArray: pass

    def preprocessInputs(self, request: TaskRequest, op: OpNode, inputs: List[EDASArray], atts: Dict[str,Any] ) -> EDASDataset:
        domains: Set[str] = EDASArray.domains( inputs, op.domain )
        if self.simpleOp(op) or (len(domains) < 2):
            return EDASDataset.init( inputs, atts )
        else:
            merged_domain: str  = request.intersectDomains( domains )
            result: EDASDataset = EDASDataset.empty()
            for input in inputs: result.addArray( input.subset( request.domain( merged_domain ) ), atts )
            return result.align( op.getParm("align","lowest") )

    def mergeEnsembles(self, request: TaskRequest, op: OpNode, inputDset: EDASDataset) -> EDASDataset:
        if op.ensDim is None: return inputDset
        sarray: xr.DataArray = xr.concat( inputDset.xarrays, dim=op.ensDim )
        result = EDASArray( inputDset.inputs[0].domId, sarray )
        return EDASDataset.init( [result], inputDset.attrs )

    def simpleOp(self, op: OpNode ) -> bool:
        alignmentStrategy = op.getParm("align")
        return op.domain is None and alignmentStrategy is None and op.ensDim is None

class InputKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("input", "Data Input","Data input and workflow source node" ) )

    def buildWorkflow(self, request: TaskRequest, node: WorkflowNode, inputs: List[EDASDataset]) -> EDASDataset:
        snode: SourceNode = node
        dataSource: DataSource = snode.varSource.dataSource
        result: EDASDataset = EDASDataset.empty()
        if dataSource.type == SourceType.collection:
            collection = Collection.new( dataSource.address )
            aggs = collection.sortVarsByAgg( snode.varSource.vids )
            for ( aggId, vars ) in aggs.items():
                dset = xr.open_mfdataset( collection.pathList(aggId), autoclose=True, data_vars=vars, parallel=True)
                result += self.processDataset( request, dset, snode )
        elif dataSource.type == SourceType.file:
            self.logger.info( "Reading data from address: " + dataSource.address )
            dset = xr.open_mfdataset(dataSource.address, autoclose=True, data_vars=snode.varSource.ids(), parallel=True)
            result += self.processDataset( request, dset, snode )
        elif dataSource.type == SourceType.dap:
            dset = xr.open_dataset( dataSource.address, autoclose=True  )
            result  +=  self.processDataset( request, dset, snode )
        return result

    def processDataset(self, request: TaskRequest, dset: xr.Dataset, snode: SourceNode ) -> EDASDataset:
        coordMap = Axis.getDatasetCoordMap( dset )
        edset: EDASDataset = EDASDataset.new( dset, { id:snode.domain for id in snode.varSource.ids() }, snode.varSource.name2id(coordMap) )
        return edset.subset( request.domain( snode.domain ) )