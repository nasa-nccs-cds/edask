from abc import ABCMeta, abstractmethod
import logging, random, string, time, socket, threading, os
from edask.process.task import TaskRequest
from typing import List, Dict, Set, Any, Optional
from edask.process.operation import WorkflowNode, SourceNode, OpNode
from edask.collections.agg import Archive
import xarray as xr
from edask.workflow.data import KernelSpec, EDASDataset, EDASArray, EDASDatasetCollection
from edask.process.source import SourceType, DataSource
from edask.process.node import Param, Node
from edask.collections.agg import Collection
from edask.config import EdaskEnv
from edask.util.logging import EDASLogger
from edask.data.cache import EDASKCacheMgr
from edask.process.domain import Domain, Axis
from collections import OrderedDict
from requests import Session

class Kernel:

    __metaclass__ = ABCMeta

    def __init__( self, spec: KernelSpec ):
        self.logger = EDASLogger.getLogger()
        self._spec: KernelSpec = spec
        self.parent: Optional[str] = None
        self._minInputs = 1
        self._maxInputs = 100000
        self.requiredOptions = []
        self._id: str  = self._spec.name + "-" + ''.join([ random.choice( string.ascii_letters + string.digits ) for n in range(5) ] )

    @property
    def name(self): return self._spec.name

    def spansInputs(self, op: WorkflowNode) -> bool:
        return (op.ensDim is not None) or ( self._minInputs > 1 )

    def requiresAlignment(self, op: WorkflowNode) -> bool:
        return self.spansInputs(op) or (op.alignmentStrategy is not None)

    def getSpec(self) -> KernelSpec: return self._spec
    def getCapabilities(self) -> str: return self._spec.xml
    def serialize(self) -> str: return self._spec.summary
    def describeProcess( self ) -> str: return str(self._spec)
    def addRequiredOptions(self, options: List[str] ): self.requiredOptions.extend(options)
    def removeRequiredOptions(self, options: List[str] ):
        for opt in options: self.requiredOptions.remove(opt)

    def testOptions(self, node: WorkflowNode):
        for option in self.requiredOptions:
            assert node.findParm( option, None ) is not None, "Option re[{}] is required for the {} kernel".format( option, self.name )

    def getResultDataset(self, request: TaskRequest, node: WorkflowNode, inputs: EDASDatasetCollection ) -> EDASDatasetCollection:
        print( " $$$$ getResultDataset: " + node.name + " -> " + inputs.arrayIds )
        results = request.getCachedResult( self._id )
        if results is None:
           results: EDASDatasetCollection = self.buildWorkflow( request, node, inputs )
           request.cacheResult( self._id, results )
        return results

    def getParameters(self, node: Node, parms: List[Param])-> Dict[str,Any]:
        return { parm.name: node.getParam(parm) for parm in parms }

    def signResult(self, result: EDASDataset, request: TaskRequest, node: WorkflowNode, **kwargs ) -> EDASDataset:
        result["proj"] = request.project
        result["exp"] = request.experiment
        result["uid"] = str(request.uid)
        for key,value in kwargs.items(): result[key] = value
        archive = node.getParm("archive")
        if archive: result["archive"] = archive
        if node.isBranch:
            result.persist()
        return result

    def archivePath(self, id: str, attrs: Dict[str, Any] )-> str:
        return Archive.getFilePath( attrs["proj"], attrs["exp"], id )

    @abstractmethod
    def buildWorkflow(self, request: TaskRequest, node: WorkflowNode, inputs: EDASDatasetCollection ) -> EDASDatasetCollection: pass

class OpKernel(Kernel):
    # Operates independently on sets of variables with same index across all input datasets
    # Will independently pre-subset to intersected domain and pre-align all variables in each set if necessary.

    def __init__(self, spec: KernelSpec):
        super(OpKernel, self).__init__(spec)
        self.addRequiredOptions( ["input"] )

    def getInputCrossSections(self, inputs: EDASDatasetCollection ) -> Dict[str,EDASDataset]:
        inputCrossSections = {}
        for dsKey, dset in inputs.items():
            for index, (akey, array) in enumerate(dset.arrayMap.items()):
                merge_set: EDASDataset = inputCrossSections.setdefault( index, EDASDataset(OrderedDict(), inputs.attrs ) )
                merge_set[dsKey + "-" + akey] = array
        return inputCrossSections

    def buildWorkflow(self, request: TaskRequest, wnode: WorkflowNode, inputs: EDASDatasetCollection ) -> EDASDatasetCollection:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( [ str(w) for w in op.inputs ] ) + ", op metadata = " + str(op.metadata) + ", axes = " + str(op.axes) )
        results = EDASDatasetCollection("OpKernel.build-" + wnode.name )
#        if (len(inputs) < self._minInputs) or (len(inputs) > self._maxInputs): raise Exception( "Wrong number of inputs for kernel {}: {}".format( self._spec.name, len(inputs)))
        self.testOptions( wnode )
        for connector in wnode.connectors:
            inputDatasets: Dict[str,EDASDataset] = self.getInputCrossSections( inputs.filterByConnector(connector) )
            for key, dset in inputDatasets.items():
                processedInputs = self.preprocessInputs(request, op, dset )
                if wnode.ensDim is not None: processedInputs = self.mergeEnsembles( op, processedInputs )
                results[connector.output] = self.processInputCrossSection( request, op, processedInputs )
        return results

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputs: EDASDataset  ) -> EDASDataset:
        resultArrays = [ array for input in inputs.arrayMap.values() for array in self.transformInput( request, node, input ) ]
        return self.buildProduct( inputs.id, request, node, resultArrays, inputs.attrs )

    def buildProduct(self, dsid: str, request: TaskRequest, node: OpNode, result_arrays: List[EDASArray], attrs: Dict[str,str] ):
        result_dset = EDASDataset.init( self.renameResults(result_arrays,node), attrs )
        for parm in [ "product", "archive" ]: result_dset[parm] = node.getParm( parm, "" )
        result_dset.name = node.getResultId( dsid )
        return self.signResult( result_dset, request, node )

    def renameResults(self, results: List[EDASArray], node: OpNode  ) -> "OrderedDict[str,EDASArray]":
        resultMap: OrderedDict[str,EDASArray] = {}
        for result in results:
            result["rid"] = node.getResultId(result.name)
            resultMap[result.name] = result
        return resultMap

    def transformInput( self, request: TaskRequest, node: OpNode, inputs: EDASArray ) -> List[EDASArray]:
        results = self.processVariable( request, node, inputs )
        for result in results: result.propagateHistory( inputs )
        return results

    def processVariable( self, request: TaskRequest, node: OpNode, inputs: EDASArray ) -> List[EDASArray]:
        return [inputs]

    def preprocessInputs( self, request: TaskRequest, op: OpNode, inputDataset: EDASDataset ) -> EDASDataset:
#         interp_na = bool(op.getParm("interp_na", False))
#         if interp_na:   inputs: Dict[str,EDASArray] = { id: input.updateXa( input.xr.interpolate_na( dim="t", method='linear' ),"interp_na" ) for (id, input) in inputDset.arrayMap.items() }
#         else:           inputs: Dict[str,EDASArray] = { id: input for (id, input) in inputDset.arrayMap.items() }
        if op.isSimple and not self.requiresAlignment:
           return inputDataset
        else:
            resultArrays: OrderedDict[str,EDASArray] = OrderedDict()
            arrayList = list(inputDataset.arrayMap.values())
            for aid,array in inputDataset.arrayMap.items():
                unapplied_domains: Set[str] = array.unapplied_domains(arrayList, op.domain)
                if len( unapplied_domains ) > 0:
                    merged_domain: str = request.intersectDomains(unapplied_domains, False)
                    processed_domain: Domain = request.cropDomain(merged_domain, arrayList )
                    sub_array = array.subset( processed_domain, unapplied_domains )
                    resultArrays[aid] = sub_array
                else:
                    resultArrays[aid] = array
            resultDataset = EDASDataset( resultArrays, inputDataset.attrs )
            alignmentTarget = resultDataset.getAlignmentVariable( op.getParm("align","lowest") )
            preprop_result = resultDataset.align( alignmentTarget )
        result: EDASDataset = preprop_result.groupby( op.grouping ).resample( op.resampling )
        print( " $$$$ processInputCrossSection: " + op.name + " -> " + str( result.ids ) )
        return result

    def mergeEnsembles(self, op: OpNode, dset: EDASDataset ) -> EDASDataset:
        sarray: xr.DataArray = xr.concat( dset.xarrays, dim=op.ensDim)
        result = EDASArray( dset.id, list(dset.domains)[0], sarray )
        return  EDASDataset.init( OrderedDict([(dset.id,result)]), dset.attrs )

# class EnsOpKernel(OpKernel):
#     # Operates independently on sets of variables with same index across all input datasets
#     # Will independently pre-subset to intersected domain and pre-align all variables in each set if necessary.
#
#     def buildWorkflow(self, request: TaskRequest, wnode: WorkflowNode, inputs: EDASDatasetCollection ) -> EDASDatasetCollection:
#         op: OpNode = wnode
#         self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( [ str(w) for w in op.inputs ] ) + ", op metadata = " + str(op.metadata) + ", axes = " + str(op.axes) )
#         result: EDASDataset = EDASDataset.empty()
# #        if (len(inputs) < self._minInputs) or (len(inputs) > self._maxInputs): raise Exception( "Wrong number of inputs for kernel {}: {}".format( self._spec.name, len(inputs)))
#         input_vars: List[List[(str,EDASArray)]] = [ list(dset.arrayMap.items()) for dset in inputs ]
#         self.testOptions( wnode )
#         matching_groups = zip( *input_vars ) if len(inputs) > 1 else [ sum( input_vars, [] ) ]
#         for matched_inputs in matching_groups:
#             inputVars: EDASDataset = self.preprocessInputs(request, op, { key:value for (key,value) in matched_inputs}, inputs[0].attrs )
#             inputCrossSection: EDASDataset = self.mergeEnsembles(request, op, inputVars)
#             product = self.processInputCrossSection( request, op, inputCrossSection )
#             for parm in [ "product", "archive" ]: product[parm] = op.getParm( parm, "" )
#             product.name = op.getResultId( inputVars.id )
#             result += product
#         return self.signResult(result,request,wnode)
#
#     def __init__(self, spec: KernelSpec):
#         super(EnsOpKernel, self).__init__(spec)
#         self.removeRequiredOptions( ["ax.s"] )
#         self._minInputs = 2
#         self._maxInputs = 2

class TimeOpKernel(OpKernel):
    # Operates independently on sets of variables with same index across all input datasets
    # Will independently pre-subset to intersected domain and pre-align all variables in each set if necessary.   , products: List[EDASDataset]

    def __init__(self, spec: KernelSpec):
        super(TimeOpKernel, self).__init__(spec)

class CacheStatus:
    Ignore = 0
    Option = 1
    Required = 2

    @classmethod
    def parse( cls, parm: str ) -> int:
        if parm is not None:
            if parm.lower().startswith("opt"): return cls.Option
            if parm.lower().startswith("req"): return cls.Required
            if parm.lower().startswith("ig"): return cls.Ignore
        assert not parm, "Unrecognized cache status: " + parm
        return cls.Ignore

class InputKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("input", "Data Input","Data input and workflow source node" ) )

    def getCacheStatus( self, node: WorkflowNode ) -> int:
        return CacheStatus.parse( node.getParm( "cache" ) )

    def getCachedDataset(self, snode: SourceNode )-> Optional[EDASDataset]:
        cache_status = self.getCacheStatus( snode )
        if cache_status != CacheStatus.Ignore:
            cid = snode.varSource.getId()
            variable = EDASKCacheMgr[ cid ]
            if variable is None:
                assert cache_status == CacheStatus.Option, "Missing cached input: " + cid
            else:
                return EDASDataset.init( OrderedDict( [ (cid, variable) ] ), {} )
        return None

    def importToDatasetCollection(self, collection: EDASDatasetCollection, request: TaskRequest, snode: SourceNode, dset: xr.Dataset):
        pdest = self.processDataset(request, dset, snode)
        for vid in snode.varSource.ids: collection[vid] = pdest.subselect(vid)

    def buildWorkflow(self, request: TaskRequest, node: WorkflowNode, inputs: EDASDatasetCollection )  -> EDASDatasetCollection:
        snode: SourceNode = node
        results = EDASDatasetCollection( "InputKernel.build-" + node.name )
        t0 = time.time()
        dset = self.getCachedDataset( snode )
        if dset is not None:
            self.importToDatasetCollection(results, request, snode, dset.xr )
            self.logger.info( "Access input data from cache: " + dset.id )
        else:
            dataSource: DataSource = snode.varSource.dataSource
            if dataSource.type == SourceType.collection:
                collection = Collection.new( dataSource.address )
                aggs = collection.sortVarsByAgg( snode.varSource.vids )
                for ( aggId, vars ) in aggs.items():
                    pathList = collection.pathList(aggId)
                    dset = xr.open_mfdataset( pathList, autoclose=True, data_vars=vars, parallel=True)
                    self.importToDatasetCollection( results, request, snode, dset )
            elif dataSource.type == SourceType.file:
                self.logger.info( "Reading data from address: " + dataSource.address )
                dset = xr.open_mfdataset(dataSource.address, autoclose=True, data_vars=snode.varSource.ids, parallel=True)
                self.importToDatasetCollection(results, request, snode, dset)
            elif dataSource.type == SourceType.archive:
                self.logger.info( "Reading data from archive: " + dataSource.address )
                dataPath =  request.archivePath( dataSource.address )
                dset = xr.open_dataset(dataPath, autoclose=True)
                self.importToDatasetCollection(results, request, snode, dset)
            elif dataSource.type == SourceType.dap:
                engine = EdaskEnv.get("dap.engine", "netcdf4")
                self.logger.info( " --------------->>> Reading data from address: " + dataSource.address + " using engine " + engine )
                session = self.getSession( dataSource )
                dap_engine = engine if session is None else "pydap"
                dset = xr.open_dataset(dataSource.address, engine=dap_engine, autoclose=True, backend_kwargs=dict(session=session) )
                self.importToDatasetCollection( results, request, snode, dset )
            self.logger.info( "Access input data source {}, time = {} sec".format( dataSource.address, str( time.time() - t0 ) ) )
            self.logger.info( "@L: LOCATION=> host: {}, thread: {}, proc: {}".format( socket.gethostname(), threading.get_ident(), os.getpid() ) )
        return results

    def getSession( self, dataSource: DataSource ) -> Session:
        session: Session = None
        if dataSource.auth == "esgf":
            from pydap.cas.esgf import setup_session
            openid = EdaskEnv.get("esgf.openid", "")
            password = EdaskEnv.get("esgf.password", "")
            username = EdaskEnv.get("esgf.username", openid.split("/")[-1] )
            session = setup_session( openid, password, username, check_url=dataSource.address )
        elif dataSource.auth == "urs":
            from pydap.cas.urs import setup_session
            username = EdaskEnv.get("urs.username", "")
            password = EdaskEnv.get("urs.password", "")
            session = setup_session( username, password, check_url=dataSource.address )
        elif dataSource.auth == "cookie":
            from pydap.cas.get_cookies import setup_session
            username = EdaskEnv.get("auth.username", "")
            password = EdaskEnv.get("auth.password", "")
            auth_url = EdaskEnv.get("auth.url", "")
            session = setup_session( auth_url, username, password )
        elif dataSource.auth is not None:
            raise Exception( "Unknown authentication method: " + dataSource.auth )
        return session

    def processDataset(self, request: TaskRequest, dset: xr.Dataset, snode: SourceNode) -> EDASDataset:
        coordMap = Axis.getDatasetCoordMap( dset )
        edset: EDASDataset = EDASDataset.new( dset, { id:snode.domain for id in snode.varSource.ids}, snode.varSource.name2id(coordMap) )
        processed_domain: Domain  = request.cropDomain( snode.domain, edset.inputs, snode.offset )
        result = edset.subset( processed_domain ) if snode.domain else edset
        return self.signResult(result, request, snode, sources=snode.varSource.getId())
