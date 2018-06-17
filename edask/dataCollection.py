import os, datetime
import sortedcontainers
from netCDF4 import MFDataset, Variable

def parse_dict( dict_spec ):
    result = {}
    for elem in dict_spec.split(","):
        elem_toks = elem.split(":")
        result[ elem_toks[0].strip() ] = elem_toks[1].strip()

class Collection:

    cacheDir = os.environ['EDAS_CACHE_DIR']
    baseDir = os.path.join( cacheDir, "collections", "agg" )

    @classmethod
    def new(cls, name ):
        # type: (str) -> Collection
        spec_file = os.path.join( cls.baseDir, name + ".csv" )
        return Collection(name, spec_file)

    def __init__(self, _name, _spec_file ):
        self.name = _name
        self.spec = _spec_file
        self.aggs = {}
        self.parms = {}
        self._parseSpecFile()

    def _parseSpecFile(self):
        file = open( self.spec, "r" )
        for line in file.readlines():
            if not line: break
            if( line[0] == '#' ):
                toks = line[1:].split(",")
                self.parms[toks[0].strip()] = ",".join(toks[1:]).strip()
            else:
                toks = line.split(",")
                self.aggs[toks[0].strip()] = ",".join(toks[1:]).strip()

    def getAggregation( self, varName ):
        # type: (str) -> Aggregation
        agg_id = self.aggs.get( varName )
        agg_file = os.path.join( Collection.baseDir, agg_id + ".ag1")
        return Aggregation( self.name, agg_file )

    def getVariable( self, varName ):
        agg =  self.getAggregation( varName )
        return agg.getVariable(varName)

    def fileList(self, varName):
        # type: (str) -> list[File]
        agg = self.getAggregation(varName)
        return agg.fileList()

    def pathList(self, varName):
        # type: (str) -> list[str]
        agg = self.getAggregation(varName)
        return agg.pathList()

class Variable:

   def __init__(self, *args ):
       self.name = args[0].strip()
       self.long_name = args[1].strip()
       self.dods_name = args[2].strip()
       self.description = args[3].strip()
       self.shape = [ int(sval.strip()) for sval in args[4].split(",") ]
       self.resolution = parse_dict( args[5] )
       self.dims = args[6].strip().split(' ')
       self.units = args[7].strip()

class Axis:

   def __init__(self, *args ):
       self.name = args[0].strip()
       self.long_name = args[1].strip()
       self.type = args[2].strip()
       self.length = int(args[3].strip())
       self.units = args[4].strip()
       self.bounds = [ float(args[5].strip()), float(args[6].strip()) ]

class File:

    def __init__(self, _collection, *args ):
       self.collection = _collection
       self.start_time = float(args[0].strip())
       self.size = int(args[1].strip())
       self.relpath = args[2].strip()
       self.date = datetime.datetime.utcfromtimestamp(self.start_time*60)

    def getPath(self):
        return os.path.join( self.parm("base.path"), self.relpath )

    def parm(self, key ):
        return self.collection.parm( key )

class Aggregation:

    def __init__(self, _name, _agg_file ):
        self.name = _name
        self.spec = _agg_file
        self.parms = {}
        self.files = sortedcontainers.SortedDict()
        self.axes = {}
        self.dims = {}
        self.vars = {}
        self._parseAggFile()

    def _parseAggFile(self):
        file = open( self.spec, "r" )
        for line in file.readlines():
            if not line: break
            toks = line.split(";")
            type = toks[0]
            if type == 'P': self.parms[ toks[1].strip() ] = ";".join( toks[2:] ).strip()
            elif type == 'A': self.axes[ toks[3].strip() ] = Axis( *toks[1:] )
            elif type == 'C': self.dims[ toks[1].strip() ] = int( toks[2].strip() )
            elif type == 'V': self.vars[ toks[1].strip() ] = Variable( *toks[1:] )
            elif type == 'F': self.files[ toks[1].strip() ] = File( self, *toks[1:] )

    def parm(self, key ):
        return self.parms.get( key, "" )

    def fileList(self):
        # type: () -> list[File]
        return self.files.values()

    def pathList(self):
        # type: () -> list[str]
        return [ file.getPath() for file in self.files.values() ]

    def getVariable( self, varName ):
        # type: (str) -> Variable
        ds = self.getDataset()
        return ds.variables[varName]

    def getDataset( self ):
        # type: () -> MFDataset
        return MFDataset( self.pathList() )