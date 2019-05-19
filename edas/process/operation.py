from typing import  List, Dict, Any, Sequence, Union, Optional, Iterator, Set, Tuple
from edas.process.source import VariableManager, VariableSource
from edas.process.domain import DomainManager, Domain, Axis
from edas.process.node import Node
from edas.data.processing import Analytics, Parser
import abc, re


class OperationConnector(Node):
   __metaclass__ = abc.ABCMeta

   def __init__(self, name: str ):
       super(OperationConnector, self).__init__(name)
       self._outputNodes: Set["WorkflowNode"] = set()

   @property
   def output(self) -> str: return self.name

   @property
   def outputNodes(self) -> "List[WorkflowNode]":
       return list(self._outputNodes)

   def hasOutput(self, connId: str) -> bool:
       return self.name == connId

   def addOutput(self, connNode: "WorkflowNode"):
       self._outputNodes.add( connNode )

   @abc.abstractmethod
   def hasInput(self, connId: str) -> bool: pass

   @property
   @abc.abstractmethod
   def inputs(self)-> List[str]: pass

   @property
   @abc.abstractmethod
   def inputNodes(self) -> "List[WorkflowNode]": pass

   @abc.abstractmethod
   def addInput(self, connNode: "WorkflowNode" ): pass

   @property
   def isResult(self) -> bool: return len( self._outputNodes ) == 0

class SourceConnector(OperationConnector):

    def __init__( self, name: str, _source: VariableSource ):
        super(SourceConnector, self).__init__(name)
        self.source = _source

    def __str__(self):
        return "SI({})[ {} ]".format(self._name, str(self.source))

    def hasInput(self, connId: str) -> bool: return False

    @property
    def inputs(self)-> List[str]: return []

    @property
    def inputNodes(self) -> "List[WorkflowNode]": return []

    def addInput(self, connNode: "WorkflowNode" ):
        raise Exception( "Can't add input to source node: {} -> {}".format( connNode.name, self.name ) )


class WorkflowConnector(OperationConnector):

    def __init__( self, output: str, inputs: List[str] ):
        super(WorkflowConnector, self).__init__(output)
        self._inputs = inputs

        self._inputNodes: List[WorkflowNode] = []

    def setConnection(self, inputNode: 'WorkflowNode', updateName = False):
        self._connection = inputNode
        if updateName and isinstance( inputNode, OpNode ):
            opNode: OpNode = inputNode
            self._name = opNode.rid

    def hasInput(self, connId: str) -> bool:
        return connId in self._inputs

    @property
    def inputs(self)-> List[str]: return self._inputs

    @property
    def inputNodes(self) -> List["WorkflowNode"]:
        return self._inputNodes

    def isConnected(self): return (len(self.inputs) > 0)

    def __str__(self):
        return "WI({})[ inputs: {} ]".format( self.name, ",".join(self._inputs) )

    def addInput(self, connNode: "WorkflowNode" ):
        self._inputNodes.append( connNode )

class MasterNodeWrapper:

    def __init__(self, _node: Optional["MasterNode"] = None ):
        self.node = _node

class WorkflowNode(Node):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name: str, _domain: str, metadata: Dict[str,Any] ):
        from edas.process.task import Job
        super(WorkflowNode, self).__init__( name, metadata )
        self.domain: str = _domain
        nameToks = name.split(":") if ":" in name else name.split(".")
        self.module: str = nameToks[0]
        assert len( nameToks ) > 1, "Expected '<module>.<operation>' format, received: '{}'".format(name)
        self.op: str = nameToks[1]
        self.axes: List[str] = self._getAxes("axis") + self._getAxes("axes")
        self._connectors: List[OperationConnector] = []
        self._masterNode: MasterNodeWrapper = None
        self._addWorkflowConnectors()
        self._instanceId = self.name + "-" + Job.randomStr(6)

    @property
    def instanceId(self) -> str:
        return self._instanceId

    @property
    def outputNodes(self) -> List["WorkflowNode"]:
        outputs = set()
        for conn in self.connectors:
            outputs.update( conn.outputNodes )
        return list( outputs )

    @property
    def inputNodes(self) -> List["WorkflowNode"]:
        inputs = set()
        for conn in self.connectors:
            inputs.update( conn.inputNodes )
        return list( inputs )

    @property
    def inputs(self) -> List[str]:
        inputs = set()
        for conn in self.connectors: inputs.update( conn.inputs )
        return list( inputs )

    @property
    def outputs(self) -> List[str]:
        return list(set([ conn.output for conn in self.connectors ]))

    @property
    def isBranch(self)->bool: return len( self.outputNodes ) > 1

    @property
    def proxyProcessed(self)->bool: return self._masterNode is not None

    @property
    def masterNode(self)-> "MasterNode": return self._masterNode.node

    @property
    def connectors(self)-> List[OperationConnector]: return self._connectors

    def findOutput( self, connId: str ) -> Optional[OperationConnector]:
        for conn in self._connectors:
            if conn.hasOutput( connId ): return conn
        return None


    def findInput( self, connId: str ) -> Optional[OperationConnector]:
        for conn in self._connectors:
            if conn.hasInput( connId ): return conn
        return None

    @masterNode.setter
    def masterNode(self, value: "MasterNode" ): self._masterNode = MasterNodeWrapper(value)

    def parseInputs(self) -> List[WorkflowConnector]:
        from edas.portal.parsers import WpsCwtParser
        connectors = []
        defaultOutput =  self.metadata.get( "result", WpsCwtParser.randomStr(6) )
        ispecs = self.metadata.get( "input", "" )
        if ispecs:
            parsed_ispecs = WpsCwtParser.parseOpConnections( ispecs )
            nOut = len( parsed_ispecs )
            for connector_spec in parsed_ispecs:
                output: str = connector_spec[1][0] if len( connector_spec ) > 1 else ( WpsCwtParser.randomStr(6) if (nOut > 1) else defaultOutput )
                connectors.append( WorkflowConnector( output, connector_spec[0] ) )
        return connectors

    def _addWorkflowConnectors(self):
        self._connectors.extend(self.parseInputs())

    def addConnector(self, input: OperationConnector):
        self._connectors.append(input)

    @property
    def domset(self) -> Set[str]: return set() if not self.domain else { self.domain }

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

    @property
    def isSimple( self ) -> bool:
        return (self.alignmentStrategy is None) and (self.ensDim is None) and not self.domain

    @abc.abstractmethod
    def getId(self): pass

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

    def __init__( self, name: str, domain: str, src: VariableSource, outputId: str, metadata: Dict[str,Any] ):
        super( SourceNode, self ).__init__( name, domain, metadata)
        self.varSource = src
        self.metadata.update( src.metadata )
        self.addConnector( WorkflowConnector( outputId, [] ) )

    @property
    def offset(self): return self.varSource.metadata.get("offset")

    def getId(self):
        return self.name

    def isResult(self):
        return False

class OpNode(WorkflowNode):

    @classmethod
    def new(cls, operationSpec: Dict[str, Any] ):
        name = operationSpec.get( "name", operationSpec.get("epa", None ) )
        assert name is not None, "Operation spec must have 'name' or 'epa' parameter: " + str( operationSpec )
        domain = operationSpec.get("domain","")
        return OpNode(name, domain, operationSpec)

    def __init__(self, name: str, domain: str, metadata: Dict[str,Any] ):
        super( OpNode, self ).__init__( name, domain, self.filterMetadata(metadata) )
        self.logger.info (" Create OpNode: " + str(self.metadata))

    def filterMetadata(self, metadata: Dict[str,Any] ) -> Dict[str,Any]:
        return { k: v for k, v in metadata.items() if v != "" }


    def isResult(self):
        for conn in self.connectors:
            if conn.isResult: return True
        return False

    def getResultId(self, varName: str ) -> str:
        return self.name +  "[" +  ",".join( self.outputs ) +  "]"

    def serialize(self) -> str:
        return "{}|{}|{}".format( self.name, self.domain, Parser.sdict(self.metadata) )

    @staticmethod
    def deserialize( spec: str ) -> "OpNode":
        toks = spec.split('|')
        return OpNode( toks[0], toks[1], "", Parser.rdict(toks[2]) )

class MasterNode(OpNode):

    def __init__(self, name: str, metadata=None):
        super(MasterNode, self).__init__( name, "", "", metadata )
        if metadata is None:
            metadata = {}
        self.proxies: Set[OpNode] = set()
        self.master_inputs: Set[WorkflowNode] = set()
        self.master_outputs: Set[OpNode] = set()

    def getMasterInputConnectiouns(self) -> Set[WorkflowConnector]:
        connections: Set[WorkflowConnector] = set()
        for proxy in self.proxies:
            for connector in proxy.connectors:
                for inputNode in connector.inputs:
                    if inputNode in self.master_inputs:
                       connections.add(connector)
        return connections

    def getInputProxies(self) -> List[WorkflowNode]:
        input_node_candidates = [internal_node_candidate for external_node in self.master_inputs for internal_node_candidate in external_node.outputs]
        input_proxies = list( filter( lambda node: node in self.proxies, input_node_candidates) )
        for input_proxy in input_proxies: self.axes.extend( input_proxy.axes )
        return input_proxies

    def getMasterOutputRids(self) -> List[str]:
        outputRids: Set[str] = set()
        for proxy in self.proxies:
            outputNodes = filter( lambda node: node not in self.proxies, proxy.outputNodes )
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
        for inputConnector in self.getMasterInputConnectiouns():  self.addConnector(inputConnector)
        for outputNode in self.getMasterOutputs():
            for connector in outputNode.connectors:
                for inputNode in connector.inputNodes:
                    if inputNode in self.proxies:
                        pass
#                        connector.setConnection(self,True)
#                        self.addOutput( outputNode )


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

    def addInputOperations(self):
        for varSource in self.variables.getVariableSources():
            for id in varSource.ids:
                op = SourceNode( "edas.input", varSource.domain, varSource, id, {} )
                self.operations.append( op )

    def getDomain( self, name: str ) -> Domain:
        return self.domains.getDomain( name )

    def findOperationByOutput(self, outputId ) -> Tuple[Optional[WorkflowNode],Optional[OperationConnector]]:
        for op in self.operations:
            conn: Optional[OperationConnector] = op.findOutput( outputId )
            if conn is not None: return ( op, conn )
        return ( None, None )

    def __str__(self):
        return "OperationManager[ {} ]:\n\t\t{}\n\t\t{}".format( "; ".join( [ str(op) for op in self.operations ] ), str(self.domains), str(self.variables) )

    def createWorkflow(self):
        for destOp in self.operations:
            for destConn in destOp.connectors:
                for inputId in destConn.inputs:
                    if inputId:
                        sourceOp, sourceConn = self.findOperationByOutput( inputId )
                        if sourceOp is not None:
                            sourceConn.addOutput( destOp )
                            destConn.addInput( sourceOp )
                        else:
                            raise Exception( "Can't find connected operation for input {} of operation {}".format( inputId, destOp.name ))

    def getResultOperations(self) -> List[WorkflowNode]:
         return list( filter( lambda x: x.isResult(), self.operations ) )

    def getOperations(self) -> List[WorkflowNode]:
         return self.operations



#    def getkernels(self):

