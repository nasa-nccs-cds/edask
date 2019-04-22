import os, time
from datetime import datetime, timezone
from collections import OrderedDict
import numpy as np
from netCDF4 import MFDataset, Variable
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView
from edas.process.source import VID
from edas.config import EdaskEnv
import defusedxml.ElementTree as ET
from edas.util.logging import EDASLogger
def a2s( elems: List[Any], sep: str = "," )-> str: return sep.join( [ str(x) for x in elems] )

def parse_dict( dict_spec ):
    result = {}
    for elem in dict_spec.split(","):
        elem_toks = elem.split(":")
        result[ elem_toks[0].strip() ] = elem_toks[1].strip()

class Archive:

    cacheDir = EdaskEnv.TRANSIENTS_DIR
    baseDir = os.path.join( cacheDir, "results" )

    @classmethod
    def getProjectPath( cls, project: str ):
        assert project, "Missing project parameter!"
        path = os.path.join( Archive.baseDir, project )
        os.makedirs( path, mode=0o777, exist_ok=True )
        return path

    @classmethod
    def getExperimentPath( cls, project: str, experiment: str ):
        assert project, "Missing project parameter!"
        assert experiment, "Missing experiment parameter!"
        path = os.path.join( Archive.baseDir, project, experiment )
        os.makedirs( path, mode=0o777, exist_ok=True )
        return path

    @classmethod
    def getFilePath(cls, project: str, experiment: str, id: str):
        expPath = cls.getExperimentPath( project, experiment )
        assert experiment, "Missing experiment parameter!"
        return os.path.join( expPath, id + ".nc")

    @classmethod
    def getLogDir( cls ):
        path = os.path.join( Archive.baseDir, "logs" )
        os.makedirs( path, mode=0o777, exist_ok=True )
        return path

class File:

    def __init__(self, _collection, *args ):
       self.collection = _collection
       self.start_time = float(args[0].strip())
       self.size = int(args[1].strip())
       self.relpath = args[2].strip()
       self.date = datetime.fromtimestamp( self.start_time*60, tz=timezone.utc)

    def getPath(self):
        return os.path.join( self.parm("base.path"), self.relpath )

    def parm(self, key ):
        return self.collection.parm( key )

    @classmethod
    def getNumber(cls, arg: str, isInt = False ) -> int:
       try:
           result = float(arg)
           return int( result ) if isInt else result
       except: return 1 if isInt else 0.0

class Collection:

    cacheDir = EdaskEnv.COLLECTIONS_DIR
    baseDir = os.path.join( cacheDir, "collections", "agg" )

    @classmethod
    def getCollectionsList(cls):
        collList = []
        for f in os.listdir(cls.baseDir):
            if( os.path.isfile( os.path.join(cls.baseDir, f ) ) and f.endswith(".csv") ):
                collection_name = f[0:-4]
                collection = Collection.new( collection_name )
                aggSpecs = [ '<variable name="{}" agg="{}"/>'.format( vName, aggName ) for vName,aggName in collection.aggs.items( )]
                collList.append( '<collection name="{}"> {} </collection>'.format( collection_name, " ".join( aggSpecs ) ) )
        return collList

    @classmethod
    def new(cls, name: str ):
        spec_file = os.path.join( cls.baseDir, name + ".csv" )
        return Collection(name, spec_file)

    def __init__(self, _name, _spec_file ):
        self.name = _name
        self.spec = _spec_file
        self.aggs = {}
        self.parms = {}
        self._parseSpecFile()

    def _parseSpecFile(self):
        assert os.path.isfile(self.spec), "Unknown Collection: " + self.spec + ", Collections dir = " + self.baseDir
        with open( self.spec, "r" ) as file:
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

    def getAggregation( self, aggId: str ) -> "Aggregation":
        agg_file = os.path.join( Collection.baseDir, aggId + ".ag1")
        return Aggregation( self.name, agg_file )

    def getVariableSpec( self, varName: str ):
        agg =  self.getAggregation( self.getAggId( varName ) )
        return agg.toXml(varName)

    def getVariable( self, varName ) -> Variable:
        agg =  self.getAggregation( self.getAggId( varName ) )
        return agg.getVariable(varName)

    def fileList(self, aggId: str ) -> List[File]:
        agg = self.getAggregation( aggId )
        return list(agg.fileList())

    def sortVarsByAgg(self, vids: List[VID] ) -> Dict[str,List[str]]:
        bins = {}
        for vid in vids:
            agg_id = self.aggs.get(vid.name)
            assert agg_id is not None, "Can't find aggregation for variable " + vid.name
            bin = bins.setdefault( agg_id, [] )
            bin.append( vid.name )
        return bins

    def pathList(self, aggId: str ) -> List[str]:
        agg = self.getAggregation( aggId )
        return agg.pathList()

    def periodPathList(self, aggId: str, start:datetime, end:datetime ) -> List[str]:
        agg = self.getAggregation( aggId )
        return agg.periodPathList( start, end )

class Axis:

   def __init__(self, *args ):
       self.name = args[0].strip()
       self.long_name = args[1].strip()
       self.type = args[2].strip()
       self.length = File.getNumber( args[3].strip(), True )
       self.units = args[4].strip()
       self.bounds = [ File.getNumber(args[5].strip()), File.getNumber(args[6].strip()) ]

   def getIndexList( self, dset, min_value, max_value ):
        values = dset.variables[self.name][:]
        return np.where((values > min_value) & (values < max_value))

   def toXml(self):
       return '<Axis id="{}" name="{}" type="{}" size="{}" units="{}" bounds="{}"/>'.format( self.name, self.long_name, self.type, self.length, self.units, ",".join([ str(x) for x in self.bounds ]))


class VarRec:

    @staticmethod
    def new( parms: List[str]):
        metadata = {}
        metadata["shortName"] = parms[0].strip()
        metadata["longName"] = parms[1].strip()
        metadata["dodsName"] = parms[2].strip()
        metadata["description"] = parms[3].strip()
        shape = list( map( lambda x: int(x), parms[4].strip().split(",") ) )
        try: resolution = { key: float(value) for (key,value) in map( lambda x: x.split(":"), parms[5].strip().split(",") ) }
        except: resolution = {}
        dims = parms[6].strip().split(" ")
        units = parms[7].strip()
        return VarRec( parms[0], shape, resolution, dims, units, metadata )

    def __init__(self, name, shape, resolution, dims, units, metadata ):
        self.name = name
        self.shape = shape
        self.resolution = resolution
        self.dims = dims
        self.units = units
        self.metadata = metadata

    def parm(self, key ):
        return self.metadata.get( key )

    def toXml(self)-> str:
        return '<variable id="{}" name="{}" dodsName="{}" description="{}" shape="{}" dims="{}" resolution="{}" units="{}"/>'.format(
            self.name, self.metadata["longName"], self.metadata["dodsName"], self.metadata["description"], a2s(self.shape), a2s(self.dims), a2s(self.resolution), self.units )

class AggProcessing:

    @classmethod
    def mapPath( cls, path: str, pathmap: Dict[str,str] ) -> str:
        for oldpath,newpath in pathmap.items():
            if path.startswith(oldpath):
                return path.replace(oldpath,newpath,1)
        return path

    @classmethod
    def changeBasePath( cls, aggFile: str, newFile:str, pathmap: Dict[str,str] ):
        print( "Change Base Path: {} -> {}".format( aggFile, newFile ) )
        with open(newFile, "w") as ofile:
          with open(aggFile, "r") as file:
            for line in file.readlines():
                if not line: break
                if line[1] == ";":
                    type = line[0]
                    value = line[2:].split(";")
                    if type == 'P':
                        if value[0].strip() == "base.path":
                            line = "P; base.path; " + cls.mapPath( value[1].strip(), pathmap ) + "\n"
                    ofile.write( line )

    @classmethod
    def changeBasePaths( cls, aggDir: str, newDir:str, pathmap: Dict[str,str] ):
        target_files = [ f for f in os.listdir(aggDir) if f.endswith(".ag1") ]
        print( "Change Base Paths: {} -> {}".format( aggDir, newDir ) )
        fileMap = [ ( os.path.join(aggDir, f), os.path.join(newDir, f) ) for f in target_files ]
        for aggFile,newFile  in fileMap: cls.changeBasePath( aggFile, newFile, pathmap )

class Aggregation:

    def __init__(self, _name, _agg_file ):
        self.logger = EDASLogger.getLogger()
        self.name = _name
        self.spec = _agg_file
        self.parms = {}
        self.files: Dict[str,File] = OrderedDict()
        self.axes: Dict[str,Axis] = {}
        self.dims = {}
        self.vars = {}
        self._parseAggFile()

    def _parseAggFile(self):
        assert os.path.isfile(self.spec), "Unknown Aggregation: " + os.path.basename(self.spec)
        self.logger.info( "Parsing Agg file: " + self.spec )
        with open(self.spec, "r") as file:
            self.logger.info(f" ---> Reading lines")
            for line in file.readlines():
                if not line: break
                if line[1] == ";":
                    try:
                        type = line[0]
                        value = line[2:].split(";")
                        self.logger.info(f" ---> Processing Line[{type}]: {value}")
                        if type == 'P': self.parms[ value[0].replace('"',' ').strip() ] = ";".join( value[1:] ).replace('"',' ').strip()
                        elif type == 'A': self.axes[ value[2].strip() ] = Axis( *value )
                        elif type == 'C': self.dims[ value[0].strip() ] = File.getNumber( value[1].strip(), True )
                        elif type == 'V': self.vars[ value[0].strip() ] = VarRec.new( value )
                        elif type == 'F': self.files[ value[0].strip() ] = File( self, *value )
                    except Exception as err:
                        self.logger.error( "Error parsing Agg file, line: " + line )
                        raise err
        self.logger.info( f"Completed Parsing Agg spec: {len(self.files)} files, {len(self.vars)} vars")

    def toXml(self, varName: str )-> str:
        specs = []
        specs.append( self.vars[ varName ].toXml() )
        specs.extend( [ axis.toXml() for axis in self.axes.values() ] )
        for name, value in self.parms.items():
            xml_str = '<parm name="{}" value="{}"/>'.format(name, value)
            try:
                xml_elem = ET.fromstring( xml_str )
                specs.append( xml_str )
            except Exception as err:
                self.logger.warn( "Skipping parm due to xml error: " + xml_str + ", error = " + getattr(err, 'message', repr(err))  )
        return '<dataset name="{}">\n\t{}\n</dataset>'.format( self.name, "\n\t".join( specs ))

    def parm(self, key ):
        return self.parms.get( key, "" )

    def getAxis( self, atype ):
        return next((x for x in self.axes.values() if x.type == atype), None)

    def fileList(self) -> ValuesView[File]:
        return self.files.values()

    def pathList(self)-> List[str]:
        return [ file.getPath() for file in self.files.values() ]

    def periodPathList(self, start:datetime, end:datetime  )-> List[str]:
        t0 = time.time()
        paths: List[str] = []
        prev_file = None
        for file in self.files.values():
            if file.date > end: break
            if file.date >= start:
                if (len(paths) == 0) and (prev_file is not None):
                    paths.append( prev_file.getPath() )
                paths.append( file.getPath() )
            prev_file = file
        self.logger.info(f"@PPL: extracted {len(paths)} paths from {len(self.files)}: time = {time.time()-t0} sec")
        return paths

    def getVariable( self, varName: str ) -> Variable:
        ds = self.getDataset()
        return ds.variables[varName]

    def getDataset( self ) -> MFDataset:
        return MFDataset( self.pathList() )


if __name__ == "__main__":
# AggProcessing.changeBasePaths( "/dass/adm/edas/cache/collection/agg",
#                                "/dass/dassnsd/data01/sys/edas/cache/collection/agg",
#                                { "/dass/pubrepo": "/dass/dassnsd/data01/cldra/data/pubrepo" }  )
#    print( str( Collection.getCollectionsList() ) )
    c: Collection = Collection.new( "cip_cfsr_mon_1980-1995" )
    print( c.getVariableSpec("tas") )
