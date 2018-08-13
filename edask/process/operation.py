from typing import  List, Dict, Any, Sequence, Union, Optional, Iterator
from enum import Enum, auto
from .variable import VariableManager, Variable, DataSource
from .domain import DomainManager, Domain
import edask

class OperationInput:

    def __init__( self, _name: str ):
        self.name = _name

class SourceInput(OperationInput):

    def __init__( self, _name: str, _source: DataSource ):
        super(SourceInput, self).__init__( _name )
        self.source = _source

    @staticmethod
    def new( variable: Variable ) -> 'SourceInput':
        return SourceInput( variable.name, variable.source )

    def __str__(self):
        return "SI({})[ source: {} ]".format( self.name, str(self.source) )

class WorkflowInput(OperationInput):

    def __init__( self, name: str ):
        super(WorkflowInput, self).__init__( name )
        self._connection: Operation = None

    def setConnection(self, connection: 'Operation' ):
        self._connection = connection

    def getConnection( self ):
        return self._connection

    def isConnected(self): return self._connection is not None

    def __str__(self):
        return "WI({})[ connection: {} ]".format( self.name, self._connection.rid if self._connection else "UNDEF" )

class Operation:

    @classmethod
    def new(cls, operationSpec: Dict[str, Any] ):
        name = operationSpec.get("name",None)
        domain = operationSpec.get("domain",None)
        rid = operationSpec.get("result",None)

        return Operation( name, domain, rid, operationSpec )

    def __init__(self, _name: str, _domain: str, _rid: str, _metadata: Dict[str,Any] ):
        self.name = _name
        self.domain = _domain
        self.rid = _rid
        self.metadata = _metadata
        nameToks = _name.split(".")
        self.module = nameToks[0]
        self.op = nameToks[1]
        self.axes: List[str] = self._getAxes()
        self.inputs: List[OperationInput] = []
        self._addWorkflowInputs()

    def isResult(self):
        return self.rid is None

    def _addWorkflowInputs(self):
        for inputName in self.metadata.get("input","").split(","):
            if inputName: self.addInput( WorkflowInput( inputName ) )

    def addInput(self, input: OperationInput ):
        self.inputs.append( input )

    def _getAxes(self) -> List[str]:
        raw_axes = self.metadata.get("axes", [] )
        if isinstance(raw_axes, str):
            raw_axes = raw_axes.replace(" ","").strip("[]")
            if( raw_axes.find(",") >= 0 ): return raw_axes.split(",")
            else: return list(raw_axes)
        else:
            return raw_axes

    variableManager: VariableManager

    def __str__(self):
        return "Op({}:{})[ domain: {}, rid: {}, axes: {}, inputs: {} ]".format( self.name, self.op, self.domain, self.rid, str(self.axes), "; ".join( [ str(i) for i in self.inputs ] ) )

class OperationManager:

    @classmethod
    def new(cls, operationSpecs: List[Dict[str, Any]], domainManager: DomainManager, variableManager: VariableManager ):
        operations = [ Operation.new(operationSpec) for operationSpec in operationSpecs ]
        return OperationManager( operations, domainManager, variableManager )

    def __init__(self, _operations: List[Operation], domainManager: DomainManager, variableManager: VariableManager ):
        self.operations: List[Operation] = _operations
        self.domains = domainManager
        self.variables = variableManager
        self.addInputOperations()

    def addInputOperations(self):
        for variable in self.variables.getVariables():
            op = Operation( "xarray.input", variable.domain, variable.id, {} )
            op.addInput( SourceInput.new( variable ) )
            self.operations.append( op )

    def findOperationByResult(self, rid ) -> Optional[Operation]:
        return next( (op for op in self.operations if op.rid == rid), None )

    def __str__(self):
        return "OperationManager[ {} ]:\n\t\t{}\n\t\t{}".format( "; ".join( [ str(op) for op in self.operations ] ), str(self.domains), str(self.variables) )

    def createWorkflow(self):
        for operation in self.operations:
            for input in operation.inputs:
                if isinstance( input, WorkflowInput ) and not input.isConnected():
                    connection = self.findOperationByResult( input.name )
                    if connection is not None: input.setConnection( connection )
                    else: raise Exception( "Can't find connected operation for input {} of operation {}".format( input.name, operation.name ))
                    
    def getResultOperations(self) -> List[Operation]:
         return list( filter( lambda x: x.isResult(), self.operations ) )


#    def getkernels(self):

