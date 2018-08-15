from typing import  List, Dict, Any, Sequence, Union, Optional, ValuesView, Tuple
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

class VID:

   def __init__(self, _name: str, _id: str ):
        self.name = _name
        self.id = _id

   def elem(self) -> Tuple[str,str]: return ( self.name, self.id  )

   def __str__(self):
        return "{}:{}".format( self.name, self.id )

class VariableSource:

    @classmethod
    def new(cls, variableSpec: Dict[str, Any] ):
        varnames = variableSpec.get("name").split(",")
        vars = []
        for varname in varnames:
            nameToks = varname.split(":")
            name = nameToks[0]
            id = nameToks[-1]
            vars.append(VID(name, id))
        domain = variableSpec.get("domain")
        source = DataSource.new( variableSpec )
        return VariableSource(vars, domain, source)

    def __init__(self, vars: List[VID], _domain: str, _source: DataSource):
        self.vids: List[VID] = vars
        self.domain: str = _domain
        self.dataSource: DataSource = _source

    def name2id(self, existingMap: Dict[str,str] = {} ) -> Dict[str,str]:
        existingMap.update( { v.elem() for v in self.vids } )
        return existingMap

    def getNames(self):
        return [ v.name for v in self.vids ]

    def getIds(self):
        return [ v.id for v in self.vids ]

    def providesId(self, vid: str ):
        return vid in self.getIds()

    def getId(self):
        return ":".join( self.getIds() )

    def __str__(self):
        return "V({})[ domain: {}, source: {} ]".format( ",".join([str(v) for v in self.vids]), self.domain, str(self.dataSource))

class VariableManager:

    @classmethod
    def new(cls, variableSpecs: List[Dict[str, Any]] ):
        vsources = [ VariableSource.new(variableSpec) for variableSpec in variableSpecs ]
        vmap = {}
        for vsource in vsources:
            for var in vsource.vids:
                vmap[var.id] = vsource
        return VariableManager( vmap )

    def __init__(self, _variables: Dict[str, VariableSource]):
        self.variables: Dict[str, VariableSource] = _variables

    def getVariable( self, id: str ) -> VariableSource:
        return self.variables.get( id )

    def getVariableSources(self) -> ValuesView[VariableSource]:
        return self.variables.values()

    def __str__(self):
        return "Variables[ {} ]".format( ";".join( [ str(v) for v in self.variables.values() ] ) )
