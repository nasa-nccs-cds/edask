from typing import  List, Dict, Any, Sequence, Union, Optional, Iterator
from enum import Enum, auto
from .source import VariableManager, VariableSource, DataSource
from .domain import DomainManager, Domain, Axis
import xarray as xr
import edask, abc

class OperationInput:
   __metaclass__ = abc.ABCMeta

   def __init__( self, _name: str ):
        self.name = _name

   @abc.abstractmethod
   def getResultIds(self): pass

class SourceInput(OperationInput):

    def __init__( self, _name: str, _source: VariableSource ):
        super(SourceInput, self).__init__( _name )
        self.source = _source

    def __str__(self):
        return "SI({})[ {} ]".format( self.name, str(self.source) )

    def getResultIds(self):
        return self.source.ids()

class WorkflowInput(OperationInput):

    def __init__( self, name: str ):
        super(WorkflowInput, self).__init__( name )
        self._connection: WorkflowNode = None

    def setConnection(self, connection: 'WorkflowNode'):
        self._connection = connection

    def getConnection( self ) -> "WorkflowNode":
        return self._connection

    def isConnected(self): return self._connection is not None

    def getResultIds(self):
        return self._connection.getResultIds()

    def __str__(self):
        return "WI({})[ connection: {} ]".format( self.name, self._connection.getId() if self._connection else "UNDEF" )


class WorkflowNode:
    __metaclass__ = abc.ABCMeta

    def __init__(self, _name: str, _domain: str, _metadata: Dict[str,Any] ):
        self.name: str = _name
        self.domain: str = _domain
        self.metadata: Dict[str,Any] = _metadata
        nameToks = _name.split(".")
        self.module: str = nameToks[0]
        self.op: str = nameToks[1]
        self.axes: List[str] = self._getAxes()
        self.inputs: List[OperationInput] = []
        self._addWorkflowInputs()

    def _addWorkflowInputs(self):
        for inputName in self.metadata.get("input","").split(","):
            if inputName: self.addInput( WorkflowInput( inputName ) )

    def addInput(self, input: OperationInput ):
        self.inputs.append( input )

    @abc.abstractmethod
    def getId(self): pass

    @abc.abstractmethod
    def getResultIds(self): pass

    @abc.abstractmethod
    def suppliesDownstreamInput(self, inputId ): pass

    @abc.abstractmethod
    def isResult(self): pass

    def _getAxes(self) -> List[str]:
        raw_axes = self.metadata.get("axes", [] )
        if isinstance(raw_axes, str):
            raw_axes = raw_axes.replace(" ","").strip("[]")
            if( raw_axes.find(",") >= 0 ): return raw_axes.split(",")
            else: return list(raw_axes)
        else:
            return raw_axes

    def hasAxis( self, axis: str ) -> bool:
        return self.axes.count( axis ) > 0

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

    def getResultIds(self):
        return self.varSource.ids()

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

    def __init__(self, name: str, domain: str, _rid: str, metadata: Dict[str,Any] ):
        super( OpNode, self ).__init__( name, domain, metadata)
        self.rid = _rid

    def getId(self):
        return self.rid

    def getResultIds(self):
        return [ self.rid ]

    def suppliesDownstreamInput(self, inputId ):
        return self.rid == inputId

    def isResult(self):
        return self.rid is None

    def getResultId(self, varName: str ) -> str:
        nodeName = self.rid if self.rid else self.name
        return "-".join( [ nodeName, varName ] )

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
                if isinstance( input, WorkflowInput ) and not input.isConnected():
                    connection = self.findOperationByResult( input.name )
                    if connection is not None: input.setConnection( connection )
                    else: raise Exception( "Can't find connected operation for input {} of operation {}".format( input.name, operation.name ))

    def getResultOperations(self) -> List[WorkflowNode]:
         return list( filter( lambda x: x.isResult(), self.operations ) )


#    def getkernels(self):

