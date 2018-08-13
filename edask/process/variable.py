from typing import  List, Dict, Any, Sequence, Union, Optional, ValuesView
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

    def __init__(self, address: str,  type: SourceType = SourceType.UNKNOWN ):
        self.processUri( type, address )

    def processUri( self, stype: SourceType, _address: str ):
        if stype.name.lower() == "uri":
            toks = _address.split(":")
            scheme = toks[0].lower()
            if scheme == "collection":
                self.type = SourceType.collection
                self.address =  toks[1].strip("/")
            elif scheme == "http":
                self.type = SourceType.collection
                self.address = _address
            elif scheme == "file":
                self.type = SourceType.file
                self.address = toks[1]
            else:
                raise Exception( "Unrecognized scheme '{}' in url: {}".format(scheme,_address) )
        else:
            self.type = stype
            self.address = _address

    def __str__(self):
        return "DS({})[ {} ]".format( self.type.name, self.address )

class Variable:

    @classmethod
    def new(cls, variableSpec: Dict[str, Any] ):
        nameToks = variableSpec.get("name").split(":")
        name = nameToks[0]
        id = nameToks[-1]
        domain = variableSpec.get("domain")
        source = DataSource.new( variableSpec )
        return Variable( name, id, domain, source )

    def __init__(self, _name: str, _id: str, _domain: str, _source: DataSource  ):
        self.name = _name
        self.id = _id
        self.domain = _domain
        self.source = _source

    def __str__(self):
        return "V({}:{})[ domain: {}, source: {} ]".format( self.name, self.id, self.domain, str(self.source) )

class VariableManager:

    @classmethod
    def new(cls, variableSpecs: List[Dict[str, Any]] ):
        variables = [ Variable.new(variableSpec) for variableSpec in variableSpecs ]
        return VariableManager( { v.name.lower(): v for v in variables } )

    def __init__(self, _variables: Dict[str,Variable] ):
        self.variables: Dict[str,Variable] = _variables

    def getVariable( self, name: str ) -> Variable:
        return self.variables.get( name.lower() )

    def getVariables(self) -> ValuesView[Variable]:
        return self.variables.values()

    def __str__(self):
        return "Variables[ {} ]".format( ";".join( [ str(v) for v in self.variables.values() ] ) )
