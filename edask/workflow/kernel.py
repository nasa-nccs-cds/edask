from abc import ABCMeta, abstractmethod
import logging, random, string, os, datetime
from edask.process.task import TaskRequest
from typing import List, Dict, Set, Any, Optional, Tuple
from edask.process.operation import WorkflowNode, SourceNode, OpNode
from edask.collections.agg import Archive
import xarray as xa
from .data import KernelSpec, EDASDataset, EDASArray
from edask.process.source import SourceType
from edask.process.source import DataSource
from edask.process.domain import Axis
from edask.collections.agg import Collection
from edask.portal.parameters import ParmMgr
from itertools import chain

class Kernel:

    __metaclass__ = ABCMeta

    def __init__( self, spec: KernelSpec ):
        self.logger = logging.getLogger()
        self._spec: KernelSpec = spec
        self.parent: Optional[str] = None
        self._minInputs = 1
        self._maxInputs = 100000
        self.requiredOptions = []
        self._id: str  = self._spec.name + "-" + ''.join([ random.choice( string.ascii_letters + string.digits ) for n in range(5) ] )

    @property
    def name(self): return self._spec.name

    def getSpec(self) -> KernelSpec: return self._spec
    def getCapabilities(self) -> str: return self._spec.summary
    def describeProcess( self ) -> str: return str(self._spec)
    def addRequiredOptions(self, options: List[str] ): self.requiredOptions.extend(options)
    def removeRequiredOptions(self, options: List[str] ):
        for opt in options: self.requiredOptions.remove(opt)

    def testOptions(self, node: WorkflowNode):
        for option in self.requiredOptions:
            assert node.findParm( option, None ) is not None, "Option re[{}] is required for the {} kernel".format( option, self.name )

    def getResultDataset(self, request: TaskRequest, node: WorkflowNode, inputs: List[EDASDataset], products: List[str]) -> EDASDataset:
        result = request.getCachedResult( self._id )
        if result is None:
           result = self.buildWorkflow( request, node, inputs, products )
           request.cacheResult( self._id, result )
        return result

    @abstractmethod
    def buildWorkflow(self, request: TaskRequest, node: WorkflowNode, inputs: List[EDASDataset], products: List[str]) -> EDASDataset: pass

class OpKernel(Kernel):
    # Operates independently on sets of variables with same index across all input datasets
    # Will independently pre-subset to intersected domain and pre-align all variables in each set if necessary.

    def __init__(self, spec: KernelSpec):
        super(OpKernel, self).__init__(spec)
        self.addRequiredOptions( ["ax.s", "input"] )

    def buildWorkflow(self, request: TaskRequest, wnode: WorkflowNode, inputs: List[EDASDataset], products: List[str]) -> EDASDataset:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( [ str(w) for w in op.inputs ] ) + ", op metadata = " + str(op.metadata) + ", axes = " + str(op.axes) )
        result: EDASDataset = EDASDataset.empty()
        if (len(inputs) < self._minInputs) or (len(inputs) > self._maxInputs): raise Exception( "Wrong number of inputs for kernel {}: {}".format( self._spec.name, len(inputs)))
        input_vars: List[List[(str,EDASArray)]] = [ dset.arrayMap.items() for dset in inputs ]
        self.testOptions( wnode )
        for matched_inputs in zip( *input_vars ):
            inputVars: EDASDataset = self.preprocessInputs(request, op, { key:value for (key,value) in matched_inputs}, inputs[0].attrs )
            inputCrossSection: EDASDataset = self.mergeEnsembles(request, op, inputVars)
            product = self.processInputCrossSection( request, op, inputCrossSection, products )
            product.name = op.getResultId( inputVars.id )
            result += product
        return result

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset, products: List[str]  ) -> EDASDataset:
        results: List[EDASArray] = list(chain.from_iterable([ self.transformInput( request, node, input, products ) for input in inputDset.inputs ]))
        return EDASDataset.init( self.renameResults(results,node), inputDset.attrs )

    def renameResults(self, results: List[EDASArray], node: OpNode  ) -> Dict[str,EDASArray]:
        resultMap: Dict[str,EDASArray] = {}
        for result in results:
            result.name = node.getResultId(result.name)
            key = result.product if len( results ) > 1 else node.rid
            resultMap[key] = result
        return resultMap

    def transformInput( self, request: TaskRequest, node: OpNode, inputs: EDASArray, products: List[str] ) -> List[EDASArray]:
        results = self.processVariable( request, node, inputs, products )
        for result in results: result.propagateHistory( inputs )
        return results

    @abstractmethod
    def processVariable( self, request: TaskRequest, node: OpNode, inputs: EDASArray, products: List[str] ) -> List[EDASArray]: pass

    def preprocessInputs(self, request: TaskRequest, op: OpNode, inputDict: Dict[str,EDASArray], atts: Dict[str,Any] ) -> EDASDataset:
        inputList = list(inputDict.values())
        domains: Set[str] = EDASArray.domains( inputList, op.domain )
        shapes: Set[Tuple[int]] = EDASArray.shapes( inputList )
        interp_na = bool(op.getParm("interp_na", False))
        if interp_na:   inputs = { id: input.updateXa( input.xr.interpolate_na( dim="t", method='linear' ),"interp_na" ) for (id, input) in inputDict.items() }
        else:           inputs = { id: input for (id, input) in inputDict.items() }
        if op.isSimple(self._minInputs) or ( (len(domains) < 2) and (len(shapes) < 2) ):
            result: EDASDataset = EDASDataset.init( inputs, atts )
        else:
            merged_domain: str  = request.intersectDomains( domains )
            result: EDASDataset = EDASDataset.empty()
            for input in inputs.values():
                sub_array = input.subset( request.domain( merged_domain ) )
                result.addArray( sub_array.name, sub_array, atts )
            result.align( op.getParm("align","lowest") )
        return result.groupby( op.grouping ).resample( op.resampling )

    def mergeEnsembles(self, request: TaskRequest, op: OpNode, inputDset: EDASDataset) -> EDASDataset:
        if op.ensDim is None: return inputDset
        sarray: xa.DataArray = xa.concat( inputDset.xarrays, dim=op.ensDim )
        result = { inputDset.id: EDASArray( inputDset.id, inputDset.inputs[0].domId, sarray, list(inputDset.groupings) ) }
        return EDASDataset.init( result, inputDset.attrs )

class DualOpKernel(OpKernel):
    # Operates independently on sets of variables with same index across all input datasets
    # Will independently pre-subset to intersected domain and pre-align all variables in each set if necessary.

    def __init__(self, spec: KernelSpec):
        super(DualOpKernel, self).__init__(spec)
        self.removeRequiredOptions( ["ax.s"] )
        self._minInputs = 2
        self._maxInputs = 2

class TimeOpKernel(OpKernel):
    # Operates independently on sets of variables with same index across all input datasets
    # Will independently pre-subset to intersected domain and pre-align all variables in each set if necessary.   , products: List[EDASDataset]

    def __init__(self, spec: KernelSpec):
        super(TimeOpKernel, self).__init__(spec)
        self.removeRequiredOptions( ["ax.s"] )

class InputKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("input", "Data Input","Data input and workflow source node" ) )

    def buildWorkflow(self, request: TaskRequest, node: WorkflowNode, inputs: List[EDASDataset], products: List[str]) -> EDASDataset:
        snode: SourceNode = node
        dataSource: DataSource = snode.varSource.dataSource
        result: EDASDataset = EDASDataset.empty()
        if dataSource.type == SourceType.collection:
            collection = Collection.new( dataSource.address )
            aggs = collection.sortVarsByAgg( snode.varSource.vids )
            for ( aggId, vars ) in aggs.items():
                dset = xa.open_mfdataset( collection.pathList(aggId), autoclose=True, data_vars=vars, parallel=True)
                result += self.processDataset( request, dset, snode )
        elif dataSource.type == SourceType.file:
            self.logger.info( "Reading data from address: " + dataSource.address )
            dset = xa.open_mfdataset(dataSource.address, autoclose=True, data_vars=snode.varSource.ids(), parallel=True)
            result += self.processDataset( request, dset, snode )
        elif dataSource.type == SourceType.archive:
            self.logger.info( "Reading data from archive: " + dataSource.address )
            dataPath = Archive.getExperimentPath( *dataSource.address.split("/") )
            dset = xa.open_dataset( dataPath, autoclose=True )
            result += self.processDataset( request, dset, snode )
        elif dataSource.type == SourceType.dap:
            engine = ParmMgr.get("engine","netcdf4")
            dset = xa.open_dataset( dataSource.address, engine=engine, autoclose=True  )
            result  +=  self.processDataset( request, dset, snode )
        return result

    def processDataset(self, request: TaskRequest, dset: xa.Dataset, snode: SourceNode ) -> EDASDataset:
        coordMap = Axis.getDatasetCoordMap( dset )
        edset: EDASDataset = EDASDataset.new( dset, { id:snode.domain for id in snode.varSource.ids() }, snode.varSource.name2id(coordMap) )
        return edset.subset( request.domain( snode.domain, snode.offset ) ) if snode.domain else edset
