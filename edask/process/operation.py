from typing import  List, Dict, Any, Sequence, Union, Optional
from enum import Enum, auto

class Operation:

    @classmethod
    def new(cls, operationSpec: Dict[str, Any] ):
        name = operationSpec.get("name",None)
        domain = operationSpec.get("domain",None)
        inputs = operationSpec.get("input","").split(",")
        axes = operationSpec.get("axes","").split(",")
        return Operation( name, domain, inputs, axes, operationSpec )

    def __init__(self, _name: str, _domain: str, _inputs: List[str], _axes: List[str], _metadata: Dict[str,Any] ):
        self.name = _name
        self.domain = _domain
        self.inputs = _inputs
        self.axes = _axes
        self.metadata = _metadata


class OperationManager:

    @classmethod
    def new(cls, operationSpecs: List[Dict[str, Any]] ):
        operations = [ Operation.new(operationSpec) for operationSpec in operationSpecs ]
        return OperationManager( { op.name.lower(): op for op in operations } )

    def __init__(self, _operations: Dict[str,Operation] ):
        self.operations = _operations

    def getOperation( self, name: str ) -> Operation:
        return self.operations.get( name.lower() )
