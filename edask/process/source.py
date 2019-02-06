from typing import  List, Dict, Any, Sequence, Union, Optional, ValuesView, Tuple
from enum import Enum, auto
from edask.process.node import Node
from edask.portal.parsers import WpsCwtParser
from edask.config import EdaskEnv

class SourceType(Enum):
    UNKNOWN = auto()
    uri = auto()
    collection = auto()
    dap = auto()
    file = auto()
    archive = auto()

class DataSource:

    @classmethod
    def new(cls, variableSpec: Dict[str, Any] ):
        for type in SourceType:
            spec = variableSpec.get( type.name, None )
            if spec is not None:
                auth = variableSpec.get( "auth", None )
                return DataSource( spec, type, auth )
        raise Exception( "Can't find data source in variableSpec: " + str( variableSpec ) )

    def __init__(self, address: str,  type: SourceType = SourceType.UNKNOWN, auth = None ):
        self.processUri( address, type )
        self.auth = auth

    @classmethod
    def validate(cls, _address: str, stype: SourceType = SourceType.uri ):
        allowed_sources = [ r.strip() for r in EdaskEnv.get("sources.allowed", "collection,https").split(",") ]

        toks = _address.split(":")
        scheme = toks[0].lower()
        if (stype.name.lower() == "uri") and (scheme in allowed_sources):
            if scheme == "https":
                trusted_servers = [ r.strip() for r in EdaskEnv.get("trusted.dap.servers", "").split(",") ]
                for trusted_server in trusted_servers:
                    if _address.startswith( trusted_server ): return scheme, toks[1]
                raise Exception( "Attempt to access untrusted dap server: {}, use parameter 'trusted.dap.servers' in app.conf to list trusted addresses, e.g. 'trusted.dap.servers=https://aims3.llnl.gov/thredds/dodsC/'".format( _address ) )
            else:
                return scheme, toks[1]
        else: raise Exception( "Unallowed scheme '{}' in url: {}".format(scheme,_address) )

    def processUri( self, _address: str, stype: SourceType ):
        scheme, path = self.validate( _address, stype )
        if stype.name.lower() == "uri":
            if scheme == "collection":
                self.type = SourceType.collection
                self.address =  path.strip("/")
            elif scheme.startswith("http"):
                self.type = SourceType.dap
                self.address = _address
            elif scheme == "file":
                self.type = SourceType.file
                self.address = path
            elif scheme == "archive":
                self.type = SourceType.archive
                self.address = path
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

   def identity(self) -> bool: return ( self.name == self.id  )

   def __str__(self):
        return "{}:{}".format( self.name, self.id )

class VariableSource(Node):

    @classmethod
    def new(cls, variableSpec: Dict[str, Any] ):
        vids = WpsCwtParser.get( ["name", "id"], variableSpec )
        assert vids is not None, "Missing 'name' or 'id' parm in variableSpec: " + str(variableSpec)
        varnames = vids.split(",")
        vars = []
        for varname in varnames:
            nameToks = WpsCwtParser.split( ["|", ":"], varname )
            name = nameToks[0]
            id = nameToks[-1]
            vars.append(VID(name, id))
        domain = variableSpec.get("domain")
        source = DataSource.new( variableSpec )
        return VariableSource(vars, domain, source, variableSpec)

    def __init__(self, vars: List[VID], _domain: str, _source: DataSource, _metadata: Dict[str, Any] ):
        super(VariableSource,self).__init__( "VS:" + _source.address, _metadata )
        self.vids: List[VID] = vars
        self.domain: str = _domain
        self.dataSource: DataSource = _source
        self.metadata = _metadata

    def name2id(self, _existingMap: Dict[str,str] = None ) -> Dict[str,str]:
        existingMap = _existingMap if _existingMap is not None else {}
        existingMap.update( { v.elem() for v in self.vids if not v.identity() } )
        return existingMap

    def names(self) -> List[str]:
        return [ v.name for v in self.vids ]

    @property
    def ids(self) -> List[str]:
        return [ v.id for v in self.vids ]

    def providesId(self, vid: str ) -> bool:
        return vid in self.ids

    def getId(self) -> str:
        return ":".join(self.ids)

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
