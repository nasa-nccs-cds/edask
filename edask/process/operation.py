from typing import  List, Dict, Any, Sequence, Union, Optional, Iterator, Set
from .source import VariableManager, VariableSource
from .domain import DomainManager, Domain, Axis
import abc, re

class Node:

    def __init__( self, name: str, _metadata: Dict[str,Any] = {} ):
       self._name = name
       self.metadata: Dict[str,Any] = _metadata

    @property
    def name(self)->str: return self._name

    def getParm(self, key: str, default: Any = None ) -> Any:
        return self.metadata.get( key, default )

    def findParm(self, idmatch: str, default: Any = None ) -> Any:
        found = [value for id, value in self.metadata.items() if re.match(idmatch,id) ]
        return found[0] if len(found) else default

    def getParms(self, keys: List[str] ) -> Dict[str,Any]:
        return dict( filter( lambda item: item[0] in keys, self.metadata.items() ) )

    def getMetadata(self, ignore: List[str] ) -> Dict[str,Any]:
        return { key:val for key,val in self.metadata.items() if key not in ignore }

    def __getitem__( self, key: str ) -> Any: return self.metadata.get( key )
    def __setitem__(self, key: str, value: Any ): self.metadata[key] = value

class OperationConnector(Node):
   __metaclass__ = abc.ABCMeta


class SourceConnector(OperationConnector):

    def __init__( self, name: str, _source: VariableSource ):
        super(SourceConnector, self).__init__(name)
        self.source = _source

    def __str__(self):
        return "SI({})[ {} ]".format(self._name, str(self.source))


class WorkflowConnector(OperationConnector):

    def __init__( self, name: str, products: List[str] ):
        super(WorkflowConnector, self).__init__(name)
        self._connection: WorkflowNode = None
        self._products = products

    def setConnection(self, inputNode: 'WorkflowNode', updateName = False):
        self._connection = inputNode
        if updateName and isinstance( inputNode, OpNode ):
            opNode: OpNode = inputNode
            self._name = opNode.rid

    @property
    def products(self)-> List[str]: return self._products

    @property
    def connection(self) -> "WorkflowNode":
        return self._connection

    def isConnected(self): return self._connection is not None

    def __str__(self):
        return "WI({})[ connection: {} ]".format( self.name, self._connection.getId() if self._connection else "UNDEF" )

class MasterNodeWrapper:

    def __init__(self, _node: Optional["MasterNode"] = None ):
        self.node = _node

class WorkflowNode(Node):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name: str, _domain: str, metadata: Dict[str,Any] ):
        super(WorkflowNode, self).__init__( name, metadata )
        self.domain: str = _domain
        nameToks = name.split(".")
        self.module: str = nameToks[0]
        self.op: str = nameToks[1]
        self.axes: List[str] = self._getAxes("axis") + self._getAxes("axes")
        self._inputs: List[OperationConnector] = []
        self._outputs: List[WorkflowNode] = []
        self._masterNode: MasterNodeWrapper = None
        self._addWorkflowInputs()

    @property
    def proxyProcessed(self)->bool: return self._masterNode is not None

    @property
    def masterNode(self)-> "MasterNode": return self._masterNode.node

    @property
    def inputs(self)-> List[OperationConnector]: return self._inputs

    @property
    def outputs(self)-> List["WorkflowNode"]: return self._outputs

    @masterNode.setter
    def masterNode(self, value: "MasterNode" ): self._masterNode = MasterNodeWrapper(value)

    def _addWorkflowInputs(self):
        for inputName in self.metadata.get("input","").split(","):
            if inputName:
                inputNameToks = inputName.split(":")
                products = inputNameToks[1].split(",") if len(inputNameToks) > 1 else []
                self.addInput(WorkflowConnector(inputNameToks[0],products))

    def addInput(self, input: OperationConnector):
        self._inputs.append( input )

    def addOutput(self, output: "WorkflowNode"):
        self._outputs.append( output )

    @property
    def domset(self) -> Set[str]: return set() if self.domain is None else { self.domain }

    @property
    def ensDim(self) -> Optional[str]:
        for axis in self.axes:
            if axis == "e" or axis == "ens": return axis
        return None

    @property
    def alignmentStrategy(self) -> Optional[str]:
        return self.getParm("align")

    @property
    def grouping(self) -> Optional[str]:
        rv: Optional[str] = self.getParm("groupby")
        if rv is None: return None
        toks = rv.split(".")
        assert len(toks) == 2, "Malformed grouping parameter (should be 'axis.freq', e.g. 't.season'): " + rv
        axis, freq = toks[0].lower(), toks[1].lower()
        return axis + "." + freq

    @property
    def resampling(self) -> Optional[str]:
        rv: Optional[str] = self.getParm("resample")
        if rv is None: return None
        toks = rv.split(".")
        assert len(toks) == 2, "Malformed resampling parameter (should be 'axis.freq', e.g. 't.season'): " + rv
        axis, freq = toks[0].lower(), toks[1].lower()
        if freq.startswith( "season" ): freq = 'Q-FEB'
        return axis + "." + freq

    def isSimple( self, minInputs: int ) -> bool:
        return (self.alignmentStrategy is None) and (self.ensDim is None) and (minInputs < 2)

    @abc.abstractmethod
    def getId(self): pass

    @abc.abstractmethod
    def suppliesDownstreamInput(self, inputId ): pass

    @abc.abstractmethod
    def isResult(self): pass

    def _getAxes(self, key ) -> List[str]:
        raw_axes = self.metadata.get( key, [] )
        if isinstance(raw_axes, str):
            raw_axes = raw_axes.replace(" ","").strip("[]")
            if( raw_axes.find(",") >= 0 ): return raw_axes.split(",")
            else: return list(raw_axes)
        else:
            return raw_axes

    def hasAxis( self, axis: Axis ) -> bool:
        return self.axes.count( axis.name.lower() ) > 0

    variableManager: VariableManager

    def __str__(self):
        return "Op({}:{})[ domain: {}, rid: {}, axes: {}, inputs: {} ]".format( self.name, self.op, self.domain, self.rid, str(self.axes), "; ".join( [ str(i) for i in self.inputs ] ) )

class SourceNode(WorkflowNode):

    @classmethod
    def new(cls, operationSpec: Dict[str, Any] ):
        name = operationSpec.get("name",None)
        domain = operationSpec.get("domain",None)
        source = operationSpec.get("source",None)
        return SourceNode(name, domain, source, operationSpec)

    def __init__(self, name: str, domain: str, src: VariableSource, metadata: Dict[str,Any] ):
        super( SourceNode, self ).__init__( name, domain, metadata)
        self.varSource = src

    def getId(self):
        return self.varSource.getId()

    def suppliesDownstreamInput(self, inputId ):
        return self.varSource.providesId(inputId)

    def isResult(self):
        return False

class OpNode(WorkflowNode):

    @classmethod
    def new(cls, operationSpec: Dict[str, Any] ):
        name = operationSpec.get("name",None)
        domain = operationSpec.get("domain",None)
        rid: str = operationSpec.get("result",None)
        return OpNode(name, domain, rid, operationSpec)

    def __init__(self, name: str, domain: Optional[str], _rid: str, metadata: Dict[str,Any] ):
        super( OpNode, self ).__init__( name, domain, metadata)
        self.rid = _rid

    def getId(self):
        return self.rid

    def suppliesDownstreamInput(self, inputId ):
        return self.rid == inputId

    def isResult(self):
        return self.rid is None

    def getResultId(self, varName: str ) -> str:
        nodeName = self.rid if self.rid else self.name
        return "-".join( [ nodeName, varName ] )

class MasterNode(OpNode):

    def __init__(self, name: str, metadata: Dict[str,Any] = {}   ):
        super(MasterNode, self).__init__( name, None, "", metadata )
        self.proxies: Set[WorkflowNode] = set()
        self.master_inputs: Set[WorkflowNode] = set()
        self.master_outputs: Set[WorkflowNode] = set()

    def getMasterInputConnectiouns(self) -> Set[WorkflowConnector]:
        connections: Set[WorkflowConnector] = set()
        for proxy in self.proxies:
            for input in proxy.inputs:
                if isinstance( input, WorkflowConnector ):
                    wc: WorkflowConnector = input
                    if wc.connection in self.master_inputs:
                       connections.add(wc)
        return connections

    def getInputProxies(self) -> List[WorkflowNode]:
        input_node_candidates = [internal_node_candidate for external_node in self.master_inputs for internal_node_candidate in external_node.outputs]
        input_proxies = list( filter( lambda node: node in self.proxies, input_node_candidates) )
        for input_proxy in input_proxies: self.axes.extend( input_proxy.axes )
        return input_proxies

    def getMasterOutputRids(self) -> List[str]:
        outputRids: Set[str] = set()
        for proxy in self.proxies:
            outputNodes = filter( lambda node: node not in self.proxies, proxy.outputs )
            if len( list( outputNodes ) ): outputRids.add( proxy.getId() )
        return list( outputRids )

    def addProxy(self, node: WorkflowNode):
        self.proxies.add(node)
        node.masterNode = self

    def addMasterInput(self, node: WorkflowNode):
        self.master_inputs.add(node)

    def addMasterOutputs(self, outputNodes: List[WorkflowNode]):
        self.master_outputs.update(outputNodes)

    def getMasterOutputs(self) -> Set[WorkflowNode]:
        return set(filter(lambda output: output not in self.proxies, self.master_outputs))

    def overlaps(self, other: "MasterNode" )-> bool:
        return not self.proxies.isdisjoint(other.proxies)

    def absorb(self, other: "MasterNode" ):
        self.proxies.update(other.proxies)
        self.master_inputs.update(other.master_inputs)
        self.master_outputs.update(other.master_outputs)

    def spliceIntoWorkflow(self):
        outputRids = self.getMasterOutputRids()
        assert len( outputRids ) == 1, "Wrong number of outputs in Master Node {}: {}".format( self.name, len( outputRids ) )
        self.rid =  outputRids[0]
        for inputConnector in self.getMasterInputConnectiouns():  self.addInput(inputConnector)
        for outputNode in self.getMasterOutputs():
            for connection in outputNode.inputs:
                if isinstance( connection, WorkflowConnector):
                    wfconn: WorkflowConnector = connection
                    if wfconn.connection in self.proxies:
                        wfconn.setConnection(self,True)
                        self.addOutput( outputNode )


class OperationManager:

    @classmethod
    def new(cls, operationSpecs: List[Dict[str, Any]], domainManager: DomainManager, variableManager: VariableManager ):
        operations = [ OpNode.new(operationSpec) for operationSpec in operationSpecs]
        return OperationManager( operations, domainManager, variableManager )

    def __init__(self, _operations: List[WorkflowNode], domainManager: DomainManager, variableManager: VariableManager):
        self.operations: List[WorkflowNode] = _operations
        self.domains = domainManager
        self.variables = variableManager
        self.addInputOperations()

    # def getModule(self):
    #     self.module = self.operations[0].module
    #     for op in self.operations:
    #         if op.module != self.module:
    #             raise Exception( "Can't mix modules in a single request: {}, {}".format( op.module, self.module ) )

    def addInputOperations(self):
        for varSource in self.variables.getVariableSources():
            op = SourceNode( "xarray.input", varSource.domain, varSource, {} )
            self.operations.append( op )

    def getDomain( self, name: str ) -> Domain:
        return self.domains.getDomain( name )

    def findOperationByResult(self, inputId ) -> Optional[WorkflowNode]:
        return next( (op for op in self.operations if op.suppliesDownstreamInput( inputId ) ), None )

    def __str__(self):
        return "OperationManager[ {} ]:\n\t\t{}\n\t\t{}".format( "; ".join( [ str(op) for op in self.operations ] ), str(self.domains), str(self.variables) )

    def createWorkflow(self):
        for operation in self.operations:
            for input in operation.inputs:
                if isinstance(input, WorkflowConnector) and not input.isConnected():
                    connection: Optional[WorkflowNode] = self.findOperationByResult( input.name )
                    if connection is not None:
                        input.setConnection( connection )
                        connection.addOutput( operation )
                    else: raise Exception( "Can't find connected operation for input {} of operation {}".format( input.name, operation.name ))

    def getResultOperations(self) -> List[WorkflowNode]:
         return list( filter( lambda x: x.isResult(), self.operations ) )

    def getOperations(self) -> List[WorkflowNode]:
         return self.operations



#    def getkernels(self):

