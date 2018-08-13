from typing import  List, Dict, Any, Sequence, Union, Optional
from enum import Enum, auto
from .variable import VariableManager, Variable
from .domain import DomainManager, Domain

class Operation:

    @classmethod
    def new(cls, operationSpec: Dict[str, Any] ):
        name = operationSpec.get("name",None)
        domain = operationSpec.get("domain",None)
        rid = operationSpec.get("result",None)
        inputs = operationSpec.get("input","").split(",")
        axes = operationSpec.get("axes","").split(",")
        return Operation( name, domain, inputs, rid, axes, operationSpec )

    def __init__(self, _name: str, _domain: str, _inputs: List[str], _rid: str, _metadata: Dict[str,Any] ):
        self.name = _name
        self.domain = _domain
        self.inputs = _inputs
        self.rid = _rid
        self.metadata = _metadata
        nameToks = _name.split(".")
        self.module = nameToks[0]
        self.op = nameToks[1]
        self.axes = self._getAxes()

    def _getAxes(self) -> List[str]:
        raw_axes = self.metadata.get("axes", [] )
        if isinstance(raw_axes, str):
            raw_axes = raw_axes.replace(" ","").strip("[]")
            if( raw_axes.find(",") >= 0 ): return raw_axes.split(",")
            else: return list(raw_axes)
        else:
            return raw_axes


    variableManager: VariableManager

class OperationManager:

    @classmethod
    def new(cls, operationSpecs: List[Dict[str, Any]], domainManager: DomainManager, variableManager: VariableManager ):
        operations = [ Operation.new(operationSpec) for operationSpec in operationSpecs ]
        return OperationManager( operations, domainManager, variableManager )

    def __init__(self, _operations: List[Operation], domainManager: DomainManager, variableManager: VariableManager ):
        self.operations = _operations
        self.domains = domainManager
        self.variables = variableManager
        self.addInputOperations()

    def addInputOperations(self):
        for variable in self.variables.getVariables():
           self.operations.append( Operation("xarray.input", variable.domain, [], variable.id, { "source": variable.source, "name": variable.name } ) )

#    def getkernels(self):

