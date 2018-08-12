from typing import  List, Dict, Any, Sequence, Union, Optional
from enum import Enum, auto

class SourceType(Enum):
    UNKNOWN = auto()
    uri = auto()
    collection = auto()
    dap = auto()
    file = auto()

class DataSource:

    @classmethod
    def new(cls, variableSpec: Dict[str, Any] ):
        for type in SourceType:
           spec = variableSpec.get( type.name, None )
           if spec is not None:
                return DataSource( spec, type )
        raise Exception( "Can't find data source in variableSpec: " + str( variableSpec ) )

    def __init__(self, _address: str,  _type: SourceType = SourceType.UNKNOWN ):
        self.type = _type
        self._address = _address

class Variable:

    @classmethod
    def new(cls, variableSpec: Dict[str, Any] ):
        name = variableSpec.get("name")
        domain = variableSpec.get("domain")
        source = DataSource.new( variableSpec )
        return Variable( name, domain, source )

    def __init__(self, _name: str, _domain: str, _source: DataSource  ):
        self.name = _name
        self.domain = _domain
        self.source = _source


class VariableManager:

    @classmethod
    def new(cls, variableSpecs: List[Dict[str, Any]] ):
        variables = [ Variable.new(variableSpec) for variableSpec in variableSpecs ]
        return VariableManager( { v.name.lower(): v for v in variables } )

    def __init__(self, _variables: Dict[str,Variable] ):
        self.variables = _variables

    def getVariable( self, name: str ) -> Variable:
        return self.variables.get( name.lower() )
