import logging
from enum import Enum, auto
from typing import List, Dict, Any, Set, Optional, Tuple, Union
from edask.process.domain import Domain, Axis
import string, random, os, re, copy
from edask.collections.agg import Archive
import abc
import xarray as xa
from edask.data.sources.timeseries import TimeIndexer
from xarray.core.groupby import DataArrayGroupBy
import xarray.plot as xrplot
import matplotlib.pyplot as plt
import numpy.ma as ma
import numpy as np

class Extremity(Enum):
    HIGHEST = auto()
    LOWEST = auto()

    @classmethod
    def parse(cls, name: str) -> "Extremity":
        n = name.lower()
        if n.startswith("high") or n.startswith("large") or n.startswith("min"): return cls.HIGHEST
        if n.startswith("low") or n.startswith("small") or n.startswith("max"): return cls.LOWEST
        else: raise Exception( "Unrecognized parameter value: " + name )

class KernelSpec:
    def __init__( self, name, title, description, **kwargs ):
        self._name = name
        self._title = title
        self._description = description
        self._options = kwargs

    @property
    def name(self): return self._name

    @property
    def description(self): return self._description

    @property
    def title(self): return self._title

    @property
    def summary(self) -> str: return ";".join( [ self.name, self.title ] )

    def __str__(self): return ";".join( [ self.name, self.title, self.description, str(self._options) ] )

class Transformation:
    def __init__(self, type: str, **kwargs: str ):
        self.type = type
        self.parms = kwargs

class EDASArray:
    def __init__(self, name: str, _domId: Optional[str], data: Union[xa.DataArray,DataArrayGroupBy], _transforms: List[Transformation], product = None ):
        self.domId = _domId
        self._data = data
        self.name = name
        self._product = product
        self.transforms = _transforms

    @property
    def size(self) -> int: return self.xr.size

    @property
    def xr(self) -> xa.DataArray:
        if isinstance(self._data,DataArrayGroupBy): return self._data._obj
        else: return self._data

    @property
    def nd(self) -> np.ndarray: return self.xr.values

    @property
    def T(self) -> "EDASArray":
        return EDASArray( self.name, self.domId,  self.xr.T, self.transforms )

    @property
    def axes(self) -> List[str]:
        return self.xr.coords.keys()

    @property
    def dims(self) -> Tuple[str]:
        return self.xr.dims

    @property
    def name(self) -> str: return self.xr.name

    @property
    def product(self) -> str: return self._product

    def rname(self, op: str ) -> str: return op + "[" + self.name + "]"

    @name.setter
    def name(self, value): self.xr.name = value

    def xarray(self, id: str ) -> xa.DataArray:
        if isinstance(self._data,DataArrayGroupBy): return self._data._obj
        else:  return xa.DataArray(self._data.data, self._data.coords, self._data.dims, id, self._data.attrs, self._data.encoding )

    def xrDataset(self, attrs: Dict[str, Any] = None) -> xa.Dataset:
        return xa.Dataset( { self.xr.name: self.xr }, attrs=attrs )

    def propagateHistory( self, precursor: "EDASArray" ):
        if precursor.product is not None: self._product = precursor.product

    def getAxisIndex( self, dims: List[str], dimIndex: int, default: int ) -> int:
        if len( dims ) <= dimIndex: return default
        return self.xr.get_axis_num( dims[dimIndex] )

    def transpose(self, *dims: str ) -> "EDASArray":
        return EDASArray( self.name, self.domId,  self.xr.transpose(*dims), self.transforms )

    def compute(self): self.xr.compute()

    def aligned( self, other: "EDASArray" ):
        return ( self.domId == other.domId ) and ( self.xr.shape == other.xr.shape ) and ( self.xr.dims == other.xr.dims )

    def groupby( self, grouping: str ):
        return EDASArray(self.name, self.domId, self.xr.groupby(grouping), self.transforms + [ Transformation( "groupby", group=grouping ) ] )

    def resample( self, resampling:str ):
        if resampling is None: return self
        rs_items = resampling.split(".")
        kwargs = { rs_items[0]: rs_items[1] }
        return EDASArray(self.name, self.domId, self.xr.resample( **kwargs ), self.transforms + [ Transformation( "resample", **kwargs )  ] )

    def align( self, other: "EDASArray", assume_sorted=True ):
        assert self.domId == other.domId, "Cannot align variable with different domains: {} vs {}".format( self.xr.name, other.xr.name, )
        if self.aligned( other ): return self
        new_data = self.xr.interp_like( other.xr, "linear", assume_sorted )
        return self.updateXa(new_data,"align")

    def updateXa( self, new_data: xa.DataArray, name:str, rename_dict: Dict[str,str] = {}, product=None ) -> "EDASArray":
        return EDASArray( self.rname(name), self.domId, new_data.rename(rename_dict), self.transforms, product )

    def updateNp(self, np_data: np.ndarray, **kwargs) -> "EDASArray":
        xrdata = xa.DataArray( np_data, coords = kwargs.get( "coords", self.xr.coords), dims = kwargs.get( "dims", self.xr.dims ) )
        return EDASArray(self.name, self.domId, xrdata, self.transforms )

    def subset( self, domain: Domain ) -> "EDASArray":
        xarray = self.xr
        for system in [ "val", "ind" ]:
            bounds_map = dict( [ domain.slice( axis, bounds ) for (axis, bounds) in domain.axisBounds.items() if bounds.system.startswith( system ) ] )
            if (len(bounds_map)):
                if system == "val":
                    xarray = xarray.sel( bounds_map )
                    for axis,axisBound in domain.axisBounds.items():
                        xarray = axisBound.revertAxis(xarray)
                else:
                    xarray.isel( bounds_map )
        return self.updateXa(xarray,"subset")

    def filter( self, axis: Axis, condition: str ) -> "EDASArray":
        assert axis == Axis.T, "Filter only supported on time axis"
        if "=" in condition:
            period,selector = condition.split("=")
            assert period.strip().lower().startswith("mon"), "Only month filtering currently supported"
        else: selector = condition
        filter = self.xr.t.dt.month.isin( TimeIndexer.getMonthIndices( selector.strip() ) )
        new_data = self.xr.sel( t=filter )
        return self.updateXa( new_data, "filter" )

    @staticmethod
    def domains( inputs: List["EDASArray"], opDomain: Optional[str] ) -> Set[str]:
        rv = { var.domId for var in inputs }
        if opDomain is not None: rv.add( opDomain )
        return rv

    @staticmethod
    def shapes( inputs: List["EDASArray"] ) -> Set[Tuple[int]]:
        return { tuple(var.xr.shape) for var in inputs }

    def coord(self, axis: Axis):
        return self.xr.coords.get( axis.name.lower() )

    def max( self, axes: List[str] ) -> "EDASArray":
        return self.updateXa(self.xr.max(dim=axes, keep_attrs=True), "max" )

    def min( self, axes: List[str] ) -> "EDASArray":
        return self.updateXa(self.xr.min(dim=axes, keep_attrs=True), "min" )

    def mean( self, axes: List[str] ) -> "EDASArray":                          # Unweighted
        return self.updateXa(self.xr.mean(dim=axes, keep_attrs=True), "mean" )

    def ave(self, axes: List[str] ) -> "EDASArray":                           # Weighted
        weights = self.getWeights( axes )
        if weights is None:
            return self.mean(axes)
        else:
            weighted_var = self.xr * weights
            sum = weighted_var.sum( axes )
            axes.remove("y")
            norm = weights * self.xr.count( axes ) if len( axes ) else weights
            new_data =  sum / norm.sum("y")
            return self.updateXa(new_data,"ave")

    def getWeights(self, axes: List[str]  ) -> Optional[xa.Dataset]:
        if 'y' in axes:
            ycoordaxis =  self.coord(Axis.Y)
            assert ycoordaxis is not None, "Can't identify Y coordinate axis, axes = " + str( self.xr.axes() )
            return np.cos( ycoordaxis * (3.1415926536/180.0) )
        else: return None

    def median( self, axes: List[str] ) -> "EDASArray":
        return self.updateXa(self.xr.median(dim=axes, keep_attrs=True), "median" )

    def var( self, axes: List[str] ) -> "EDASArray":
        return self.updateXa(self.xr.var(dim=axes, keep_attrs=True), "var" )

    def std( self, axes: List[str] ) -> "EDASArray":
        return self.updateXa(self.xr.std(dim=axes, keep_attrs=True), "std" )

    def sum( self, axes: List[str] ) -> "EDASArray":
        return self.updateXa(self.xr.sum(dim=axes, keep_attrs=True), "sum" )

    def __sub__(self, other: "EDASArray") -> "EDASArray":
        assert self.domId == other.domId, "Can't combine arrays with different domains"
        result: xa.DataArray = self.xr - other.xr
        return self.updateXa(result, "diff")

    def __add__(self, other: "EDASArray") -> "EDASArray":
        assert self.domId == other.domId, "Can't combine arrays with different domains"
        result: xa.DataArray = self.xr + other.xr
        return self.updateXa(result, "sum")

    def __mul__(self, other: "EDASArray") -> "EDASArray":
        assert self.domId == other.domId, "Can't combine arrays with different domains"
        result: xa.DataArray = self.xr * other.xr
        return self.updateXa(result, "mul")

    def __truediv__(self, other: "EDASArray") -> "EDASArray":
        assert self.domId == other.domId, "Can't combine arrays with different domains"
        result: xa.DataArray = self.xr / other.xr
        return self.updateXa(result, "div")

    def __getitem__( self, key: str ) -> str: return self.xr.attrs.get( key )
    def __setitem__(self, key: str, value: str ): self.xr.attrs[key] = value

class EDASDataset:

    def __init__( self, _arrayMap: Dict[str,EDASArray], _attrs: Dict[str,Any]  ):
        self.arrayMap: Dict[str,EDASArray] = _arrayMap
        self.attrs = _attrs
        self.logger = logging.getLogger()

    @staticmethod
    def init( arrays: Dict[str,EDASArray], attrs: Dict[str,Any]  ) -> "EDASDataset":
        dataset = EDASDataset.empty()
        return dataset.addArrays(arrays,attrs)

    @staticmethod
    def open_dataset( filePath: str ) -> "EDASDataset":
        return EDASDataset.new(xa.open_dataset(filePath))

    @staticmethod
    def open_archive( project: str, experiment: str, type: str  ) -> "EDASDataset":
        filePath = Archive.getFilePath(project, experiment, type)
        return EDASDataset.open_dataset( filePath )

    @classmethod
    def rename( cls, dataset: xa.Dataset, idMap: Dict[str,str] = {} ) -> xa.Dataset:
        for id,val in idMap.items():
            if val not in dataset and val not in dataset.dims:
                dataset.rename( {id:val}, True )
        return dataset

    @classmethod
    def new( cls, dataset: xa.Dataset, varMap: Dict[str,str] = {}, idMap: Dict[str,str] = {} ):
        cls.rename( dataset, idMap )
        if varMap:  arrayMap = { vid: EDASArray( vid, domId, dataset[vid], [] ) for ( vid, domId ) in varMap.items() }
        else:       arrayMap = { vid: EDASArray( vid, None, dataset[vid], [] ) for ( vid ) in dataset.variables.keys() }
        return EDASDataset( arrayMap, dataset.attrs )

    def addArrays(self, arrays: Dict[str,EDASArray], attrs: Dict[str,Any]  ) -> "EDASDataset":
        self.arrayMap = copy.deepcopy(arrays)
        self.attrs.update(attrs)
        return self

    def addArray(self, id: str, array: EDASArray, attrs: Dict[str,Any]  ) -> "EDASDataset":
        self.arrayMap[id] = array
        self.attrs.update(attrs)
        return self

    def save( self, id: str = None  ):
        dset = self.xr
        filePath = self.archivePath( id )
        vars: List[xa.DataArray] = dset.data_vars.values()
        dset.to_netcdf( path=filePath )
        self.logger.info( " SAVE: " + str([ x.name + ":" + str(x.shape) for x in vars ]) + " to file " + filePath )
        return filePath

    @property
    def product(self):
        for array in self.arrayMap.values():
            if array.product: return array.product
        return self.attrs.get("product",None)

    def getCoord( self, name: str ) -> xa.DataArray: return self.xr.coords[name]
    def getArray(self, id: str  ) -> EDASArray: return self.arrayMap.get(id,None)
    def customArraymap(self, id: str  ) -> Dict[str,EDASArray]: return { (key+"-"+id):item for key,item in self.arrayMap.items() }

    @property
    def domains(self) -> Set[str]: return { array.domId for array in self.arrayMap.values() }

    @property
    def ids(self) -> Set[str]: return set( self.arrayMap.keys() )

    @property
    def id(self) -> str: return "-".join( self.arrayMap.keys() )

    @property
    def xr(self) -> xa.Dataset: return xa.Dataset( { id:array.xr for id,array in self.arrayMap.items() }, attrs=self.attrs )

    @property
    def vars2doms(self) -> Dict[str,str]: return { name:array.domId for ( name, array ) in self.arrayMap.items() }

    @staticmethod
    def empty() -> "EDASDataset": return EDASDataset( {}, {} )

    @staticmethod
    def domainSet( inputs: List["EDASDataset"], opDomains: Set[str] = None ) -> Set[str]:
        rv = set()
        for dset in inputs: rv = rv.union( dset.domains )
        return rv if opDomains is None else rv | opDomains

    @staticmethod
    def mergeVarMaps( inputs: List["EDASDataset"] ) -> Dict[str,str]:
        rv = {}
        for dset in inputs: rv = rv.update( dset.vars2doms )
        return rv

    @property
    def inputs(self) -> List[EDASArray]: return list(self.arrayMap.values())

    @property
    def xarrays(self) -> List[xa.DataArray]: return [ array.xr for array in self.arrayMap.values() ]

    @property
    def groupings(self) -> Set[Transformation]: return {(grouping for grouping in array.transforms) for array in self.arrayMap.values()}

    def find_arrays(self, idmatch: str ) -> List[xa.DataArray]:
        return [ array.xr for id, array in self.arrayMap.items() if re.match(idmatch,id) is not None ]

    def compute(self):
        for ( vid, array ) in self.arrayMap.items(): array.compute()
        return self

    def subset( self, domain: Domain ):
        arrayMap = { vid: array.subset( domain ) for ( vid, array ) in self.arrayMap.items() }
        return EDASDataset( arrayMap, self.attrs )

    def groupby( self, grouping: str ):
        if grouping is None: return self
        arrayMap = { vid: array.groupby( grouping ) for ( vid, array ) in self.arrayMap.items() }
        return EDASDataset( arrayMap, self.attrs )

    def resample( self, resampling: str ) -> "EDASDataset":
        if resampling is None: return self
        arrayMap = { vid: array.resample( resampling ) for ( vid, array ) in self.arrayMap.items() }
        return EDASDataset( arrayMap, self.attrs )

    def requiresSubset(self, target_domain: str ) -> bool:
        return len( self.domains.difference( { target_domain } ) ) > 0

    def getExtremeVariable(self, ext: Extremity ) -> EDASArray:
        sizes = [ x.size for x in self.inputs ]
        exVal = max( sizes ) if ext == Extremity.HIGHEST else min( sizes )
        return self.inputs[ sizes.index( exVal ) ]

    def getAlignmentVariable(self, alignRes: str ):
        return self.getExtremeVariable( Extremity.parse(alignRes) )

    def align( self, alignRes: str = "lowest" ) -> "EDASDataset":
      if not alignRes: return self
      target_var: EDASArray =  self.getAlignmentVariable( alignRes )
      new_vars = { var.name: var.align(target_var) for var in self.inputs }
      return EDASDataset.init( new_vars, self.attrs )

    def addDataset(self, dataset: xa.Dataset, varMap: Dict[str,str] ):
        arrays = { vid:EDASArray( vid, domId, dataset[vid], [] ) for ( vid, domId ) in varMap.items() }
        self.addArrays( arrays, dataset.attrs )

    def splot(self, tindex: int = 0 ):
        for array in self.inputs:
            input_data = array.xr.isel( { "t": slice( tindex, tindex + 1 ) } ).squeeze()
            mesh = xrplot.pcolormesh( input_data )
            plt.show()

    def dplot(self, domain: Domain ):
        for array in self.inputs:
            xrplot.plot( array.subset( domain ).xr )

    def mplot(self, facet_axis: str = "time" ):
        for array in self.inputs:
            xrplot.plot( array.xr, col = facet_axis )

    def __iadd__(self, other: "EDASDataset" ) -> "EDASDataset":
        self.arrayMap.update( other.arrayMap )
        self.attrs.update( other.attrs )
        return self

    def plot(self, idmatch: str = None ):
        nplots = len( self.ids )
        fig, axes = plt.subplots(ncols=nplots)
        self.logger.info( "Plotting {} plot(s)".format(nplots) )
        if nplots == 1:
            self.xarrays[0].plot(ax=axes)
        else:
            xarrays = self.xarrays if idmatch is None else self.find_arrays(idmatch)
            for iaxis, result in enumerate( xarrays ):
                result.plot(ax=axes[iaxis])

    @classmethod
    def mergeArrayMaps( cls, amap0: Dict[str,EDASArray], amap1: Dict[str,EDASArray] )-> Dict[str,EDASArray]:
        result: Dict[str, EDASArray] = {}
        for key in amap0.keys():
            if key in amap1:
                result[ key + "-0" ] = amap0[key]
                result[ key + "-1"] =  amap1[key]
            else:
                result[key] = amap0[key]
        for key in amap1.keys():
            if key not in amap0:
                result[key] = amap1[key]
        return result

    @classmethod
    def merge(cls, dsets: List["EDASDataset"]):
        if len( dsets ) == 1: return dsets[0]
        arrayMap: Dict[str,EDASArray] = {}
        attrs: Dict[str,Any] = {}
        for idx,dset in enumerate(dsets):
            arrayMap = cls.mergeArrayMaps(  arrayMap, dset.arrayMap )
            attrs.update( dset.attrs )
        return EDASDataset( arrayMap, attrs )

    @staticmethod
    def randomStr(length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    def archivePath(self, id: str = None  ) -> str:
        proj =  self.attrs.get( "proj", self.randomStr(4) )
        exp =   self.attrs.get( "exp", self.randomStr(4) )
        id =  self.attrs.get( "archive", self.randomStr(4) )
        return Archive.getFilePath( proj, exp, id )

    def parm(self, key: str, default: str) -> Any: return self.attrs.get(key,default)
    def __getitem__( self, key: str ) -> Any: return self.attrs.get( key )
    def __setitem__(self, key: str, value: Any ): self.attrs[key] = value


class MergeHandler:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def mergeResults(self, results: List[EDASDataset] ) -> EDASDataset: pass

class StandardMergeHandler(MergeHandler):

    def mergeResults(self, results: List[EDASDataset] ) -> EDASDataset:
        return EDASDataset.merge( results )