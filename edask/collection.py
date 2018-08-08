import os, datetime
import sortedcontainers
import numpy as np
import edask
from netCDF4 import MFDataset, Variable
from typing import List, Dict, Sequence, BinaryIO, TextIO

def parse_dict( dict_spec ):
    result = {}
    for elem in dict_spec.split(","):
        elem_toks = elem.split(":")
        result[ elem_toks[0].strip() ] = elem_toks[1].strip()

class Collection:

    cacheDir = os.environ['EDAS_CACHE_DIR']
    baseDir = os.path.join( cacheDir, "collections", "agg" )

    @classmethod
    def new(cls, name: str ) -> edask.collection.Collection:
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

    def getAggId( self, varName: str ) -> str:
        return self.aggs.get( varName )

    def getAggregation( self, aggId: str ) -> edask.collection.Aggregation:
        agg_file = os.path.join( Collection.baseDir, aggId + ".ag1")
        return Aggregation( self.name, agg_file )

    def getVariable( self, varName ) -> Variable:
        agg =  self.getAggregation( varName )
        return agg.getVariable(varName)

    def fileList(self, aggId: str ) -> List[BinaryIO]:
        agg = self.getAggregation( aggId )
        return agg.fileList()

    def sortVarsByAgg(self, varNames: Sequence[str] ) -> Dict[str,List[str]]:
        bins = {}
        for varName in varNames:
            agg_id = self.aggs.get(varName)
            bin = bins.setdefault( agg_id, [] )
            bin.append( varName )
        return bins

    def pathList(self, aggId: str ) -> List[str]:
        agg = self.getAggregation( aggId )
        return agg.pathList()

# class EVariable:
#
#    def __init__(self, *args ):
#        self.name = args[0].strip()
#        self.long_name = args[1].strip()
#        self.dods_name = args[2].strip()
#        self.description = args[3].strip()
#        self.shape = [ int(sval.strip()) for sval in args[4].split(",") ]
#        self.resolution = parse_dict( args[5] )
#        self.dims = args[6].strip().split(' ')
#        self.units = args[7].strip()

class Axis:

   def __init__(self, *args ):
       self.name = args[0].strip()
       self.long_name = args[1].strip()
       self.type = args[2].strip()
       self.length = int(args[3].strip())
       self.units = args[4].strip()
       self.bounds = [ float(args[5].strip()), float(args[6].strip()) ]

   def getIndexList( self, dset, min_value, max_value ):
        values = dset.variables[self.name][:]
        return np.where((values > min_value) & (values < max_value))

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
        self.files: Dict[str,File] = sortedcontainers.SortedDict()
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

    def getAxis( self, atype ):
        return next((x for x in self.axes.values() if x.type == atype), None)

    def fileList(self) -> List[BinaryIO]:
        return self.files.values()

    def pathList(self)-> List[str]:
        return [ file.getPath() for file in self.files.values() ]

    def getVariable( self, varName: str ) -> Variable:
        ds = self.getDataset()
        return ds.variables[varName]

    def getDataset( self ) -> MFDataset:
        return MFDataset( self.pathList() )