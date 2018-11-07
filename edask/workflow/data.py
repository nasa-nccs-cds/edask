import logging
from enum import Enum, auto
from typing import List, Dict, Any, Set, Optional, Tuple, Union, ItemsView, KeysView
from edask.process.domain import Domain, Axis
import string, random, os, re, traceback
from edask.collections.agg import Archive
import abc, math, time
import xarray as xa
from dask.distributed import Client
from edask.data.sources.timeseries import TimeIndexer
from edask.util.logging import EDASLogger
from xarray.core.groupby import DataArrayGroupBy
from edask.process.operation import WorkflowNode, OperationConnector
from edask.data.processing import Parser
from collections import OrderedDict
import xarray.plot as xrplot
import cartopy.crs as ccrs
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

    @property
    def xml(self) -> str: return '<kernel name="{}" title="{}" />'.format( self.name, self.title )

    def __str__(self): return ";".join( [ self.name, self.title, self.description, str(self._options) ] )

class Transformation:
    def __init__(self, type: str, **kwargs: str ):
        self.type = type
        self.parms = kwargs

    @staticmethod
    def parse(str):
        args = str.split("|")
        kwargs = Parser.rdict( args[1] )
        return  Transformation( args[0], **kwargs )

    def __repr__(self) -> str:
        return self.type + "|" + Parser.sdict( self.parms )

class EDASArray:
    def __init__( self, name: Optional[str], _domId: Optional[str], data: Union[xa.DataArray,DataArrayGroupBy] ):
        self.alwaysPersist = False
        self.loaded_data = None
        self.logger = EDASLogger.getLogger()
        self.domId = _domId
        self._data = data
        self.name = name
        self.addDomain( _domId )

    def purge(self, rename_dict=None):
        if rename_dict is None:
            rename_dict = {}
        return EDASArray( self.name, self.domId, self.cleanupCoords( self.xr, rename_dict ) )

    @staticmethod
    def cleanupCoords(xarray: xa.DataArray, rename_dict=None) -> xa.DataArray:
        if rename_dict is None:
            rename_dict = {}
        for coord in xarray.coords:
            if coord not in xarray.dims: xarray = xarray.drop(coord)
        for item in rename_dict.items():
            try: xarray = xarray.rename( {item[0]:item[1]} )
            except: pass
        return xarray

    @property
    def domain_history(self) -> Set[str]:
        return { d for d in self.get("domain_history","").split(";") if d }

    @property
    def transforms(self) -> Set[Transformation]:
        return { Transformation.parse(t) for t in self.get("transforms","").split(";") if t }

    @property
    def size(self) -> int: return self.xr.size

    @property
    def bsize(self) -> int: return self.xr.size * 4

    @property
    def product(self) -> Optional[str]: return self.get("product",None)

    @product.setter
    def product(self, value: str ): self["product"] = value

    def persist(self) -> xa.DataArray:
        if self.loaded_data is None:
            client = None # Client.current()
            self.loaded_data = self.xr.load().persist() if client is None else Client.persist( self.xr.load() )
        return self.loaded_data

    @property
    def xr(self) -> xa.DataArray:
        if self.loaded_data is not None:
            return self.loaded_data
        else:
            return self._data._obj if isinstance(self._data,DataArrayGroupBy) else self._data

    @property
    def xrp(self) -> xa.DataArray: return self.persist()

    @property
    def nd(self) -> np.ndarray: return self.xr.values

    @property
    def T(self) -> "EDASArray":
        return EDASArray( self.name, self.domId,  self.xr.T )

    @property
    def axes(self) -> List[str]:
        return self.xr.coords.keys()

    @property
    def dims(self) -> Tuple[str]:
        return self.xr.dims

    @property
    def name(self) -> str: return self.xr.name

    def rname(self, op: str ) -> str: return op + "[" + self.name + "]"

    @name.setter
    def name(self, value):
        if value: self.xr.name = value

    def selectPoint(self, lon: float, lat: float ) -> "EDASArray":
        return EDASArray( self.name, self.domId, self.xr.sel( x=lon, y=lat, method='nearest') )

    def addDomain( self, d: str ):
        domains = self.domain_history
        if d is not None: domains.add( d )
        self.xr.attrs.setdefault( "domain_history", ";".join(domains) )

    def addTransform( self, t: Transformation ):
        transforms = self.transforms
        transforms.add( t )
        self.xr.attrs.setdefault("transforms", ";".join( [ repr(t) for t in transforms] ) )

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

    def axisLen( self, dim: str ) -> int:
        return self.xr.shape[ self.xr.get_axis_num(dim) ]

    def transpose(self, *dims: str ) -> "EDASArray":
        return EDASArray( self.name, self.domId,  self.xr.transpose(*dims) )

    def aligned( self, other: "EDASArray" ):
        return ( self.domId == other.domId ) and ( self.xr.shape == other.xr.shape ) and ( self.xr.dims == other.xr.dims )

    def groupby( self, grouping: str ):
        rv = EDASArray(self.name, self.domId, self.xr.groupby(grouping) )
        rv.addTransform( Transformation( "groupby", group=grouping ) )
        return rv

    def resample( self, resampling:str ):
        if resampling is None: return self
        rs_items = resampling.split(".")
        kwargs = { rs_items[0]: rs_items[1] }
        rv =  EDASArray(self.name, self.domId, self.xr.resample( **kwargs ) )
        rv.addTransform(  Transformation( "resample", **kwargs ) )
        return rv

    def align( self, other: "EDASArray", assume_sorted=True ):
        assert self.domId == other.domId, "Cannot align variable with different domains: {} vs {}".format( self.xr.name, other.xr.name, )
        if self.aligned( other ): return self
        new_data = self.xr.interp_like( other.xr, "linear", assume_sorted )
        return self.updateXa(new_data,"align")

    def updateXa(self, new_data: xa.DataArray, name:str, rename_dict=None, product=None) -> "EDASArray":
        if rename_dict is None:
            rename_dict = {}
        return EDASArray( self.rname(name), self.domId, new_data.rename(rename_dict)  )

    def updateNp(self, np_data: np.ndarray, **kwargs) -> "EDASArray":
        xrdata = xa.DataArray( np_data, coords = kwargs.get( "coords", self.xr.coords), dims = kwargs.get( "dims", self.xr.dims ) )
        return EDASArray(self.name, self.domId, xrdata  )

    def getSliceMaps(self, domain: Domain, dims: List[str] ) -> ( Dict[str,Any], Dict[str,slice], Dict[str,slice]):
        pointMap: Dict[str,Any] = {}
        valSliceMap: Dict[str, Any] = {}
        indexSliceMap: Dict[str, Any] = {}
        for (axis, bounds) in domain.axisBounds.items():
            axname, slice = domain.slice(axis, bounds)
            if axname in dims:
                if bounds.system.startswith("val") or bounds.system.startswith("time"):
                    if slice.start == slice.stop: pointMap[axname] = slice.start
                    else: valSliceMap[axname] = slice
                else: indexSliceMap[axname] = slice
            else:
                self.logger.warning( " Domain {} contains axis {} that is not in the variable's dimensions: {}".format( domain.name, axname, str(dims) ) )
        return pointMap, valSliceMap, indexSliceMap

    def subset( self, domain: Domain, composite_domains: Set[str] ) -> "EDASArray":
        xarray = self.xr
        pointMap, valSliceMap, indexSliceMap = self.getSliceMaps( domain, xarray.dims )
        if len(pointMap):
            self.logger.info( "POINT subset: " + str(pointMap))
            xarray = xarray.sel( pointMap, method='nearest')
        if len(valSliceMap):
            self.logger.info( "SLICE subset: " + str(valSliceMap))
            xarray = xarray.sel( valSliceMap )
        if len(indexSliceMap):
            self.logger.info( "INDEX subset: " + str(indexSliceMap))
            xarray = xarray.isel(indexSliceMap )
        for axis, axisBound in domain.axisBounds.items():  xarray = axisBound.revertAxis(xarray)
        result = self.updateXa(xarray,"subset")
        for d in composite_domains: result.addDomain( d )
        return result

    def filter( self, axis: Axis, condition: str ) -> "EDASArray":
        data = self.persist()
        assert axis == Axis.T, "Filter only supported on time axis"
        if "=" in condition:
            period,selector = condition.split("=")
            assert period.strip().lower().startswith("mon"), "Only month filtering currently supported"
        else: selector = condition
        filter = data.t.dt.month.isin( TimeIndexer.getMonthIndices( selector.strip() ) )
        new_data = data.sel( t=filter )
        return self.updateXa( new_data, "filter" )

    def unapplied_domains( self, inputs: List["EDASArray"], opDomain: Optional[str] ) -> Set[str]:
        new_domains = { var.domId for var in inputs }
        if opDomain is not None: new_domains.add( opDomain )
        return { d for d in new_domains if d and (d not in self.domain_history)  }

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
        return self.updateXa( self.xr.mean(dim=axes, keep_attrs=True), "mean" )

    def ave(self, axes: List[str] ) -> "EDASArray":                           # Weighted
        weights = self.getWeights( axes )
        if weights is None:
            return self.mean(axes)
        else:
            data = self.persist()
            weighted_var = data * weights
            sum = weighted_var.sum( axes )
            axes.remove("y")
            norm = weights * data.count( axes ) if len( axes ) else weights
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

    def get(self, key: str, default: Optional[str] ) -> str: return self.xr.attrs.get( key, default )

    def __getitem__( self, key: str ) -> str: return self.xr.attrs.get( key )
    def __setitem__(self, key: str, value: str ): self.xr.attrs[key] = value

class PlotType:
    EOF: int = 0
    PC: int = 1

class EDASDataset:
    StandardAxisMap = { "x":"lon", "y":"lat", "z":"lev", "t":"time", "e":"ens", "m":"mode" }

    def __init__( self, _arrayMap: "OrderedDict[str,EDASArray]", _attrs: Dict[str,Any]  ):
        self.arrayMap: OrderedDict[str,EDASArray] = _arrayMap
        self.attrs = _attrs
        self.logger = EDASLogger.getLogger()

    def purge(self):
        purgedArrayMap = OrderedDict()
        for id,array in self.arrayMap.items(): purgedArrayMap[id] = array.purge()
        return EDASDataset( purgedArrayMap, self.attrs )

    def persist(self) -> "EDASDataset":
        for array in self.arrayMap.values(): array.persist()
        return self

    def addDomains( self, domains: Set[str] ):
        for domain in domains:
            for array in self.arrayMap.values():
                array.addDomain( domain )

    @staticmethod
    def init( arrays: "OrderedDict[str,EDASArray]", attrs: Dict[str,Any]  ) -> "EDASDataset":
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
    def rename(cls, dataset: xa.Dataset, idMap=None) -> xa.Dataset:
        if idMap is None:
            idMap = {}
        for id,val in idMap.items():
            if val not in dataset and val not in dataset.dims:
                dataset.rename( {id:val}, True )
        return dataset

    def subselect(self, idmatch: str ) -> "EDASDataset":
        selection = OrderedDict()
        for id, array in self.arrayMap.items():
            if re.match(idmatch,id) is not None:
               selection[id] = array
        return EDASDataset( selection, self.attrs )

    def standardize(self, new_attrs=None) -> "EDASDataset":
        if new_attrs is None:
            new_attrs = {}
        dataset = self.purge().xr
        for id,val in self.StandardAxisMap.items():
            if id in dataset.dims and val not in dataset.dims:
                dataset = dataset.rename( {id:val}, True )
        result_attrs = { **self.attrs, **new_attrs }
        return self.fromXr( dataset, result_attrs )

    @classmethod
    def fromXr(cls, dataset: xa.Dataset, attrs=None) -> "EDASDataset":
        result = OrderedDict()
        if attrs is None: attrs = {}
        for id,v in dataset.data_vars.items():
            result[id] = EDASArray( None, None, v )
        return EDASDataset( result, attrs )

    @classmethod
    def new(cls, dataset: xa.Dataset, varMap=None, idMap=None):
        if varMap is None: varMap = {}
        if idMap is None: idMap = {}
        cls.rename( dataset, idMap )
        result = OrderedDict()
        if varMap:
             for ( vid, domId ) in varMap.items():
                 result[vid] = EDASArray( vid, domId, dataset[vid] )
        else:
            for ( vid ) in dataset.variables.keys():
                result[vid] = EDASArray( vid, None, dataset[vid] )
        return EDASDataset( result, dataset.attrs )

    def addArrays(self, arrays: Dict[str,EDASArray], attrs: Dict[str,Any]  ) -> "EDASDataset":
        self.arrayMap = arrays # copy.deepcopy(arrays)
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

    def customArraymap(self, id: str  ) -> "OrderedDict[str,EDASArray]":
        result = OrderedDict()
        for key,item in self.arrayMap.items():
            result[(key+"-"+id)] = item
        return result

    @property
    def domains(self) -> Set[str]: return { array.domId for array in self.arrayMap.values() }

    @property
    def ids(self) -> Set[str]: return set( self.arrayMap.keys() )

    @property
    def id(self) -> str: return "-".join( self.arrayMap.keys() )

    @property
    def xr(self) -> xa.Dataset:
        arrays = OrderedDict()
        for key,array in self.arrayMap.items(): arrays[key] = array.xr
        return xa.Dataset( arrays, attrs=self.attrs )

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
    def arrays(self) -> List[EDASArray]: return self.inputs

    @property
    def xarrays(self) -> List[xa.DataArray]: return [ array.xr for array in self.arrayMap.values() ]

    @property
    def groupings(self) -> Set[Transformation]: return {(grouping for grouping in array.transforms) for array in self.arrayMap.values()}

    def find_arrays(self, idmatch: str ) -> List[xa.DataArray]:
        return [ array.xr for id, array in self.arrayMap.items() if re.match(idmatch,id) is not None ]

    def subset( self, domain: Domain ):
        arrays = OrderedDict()
        for vid,array in self.arrayMap.items(): arrays[vid] = array.subset( domain, { domain.name } )
        return EDASDataset( arrays, self.attrs )

    def groupby( self, grouping: str ):
        if grouping is None: return self
        arrays = OrderedDict()
        for vid,array in self.arrayMap.items(): arrays[vid] = array.groupby( grouping )
        return EDASDataset( arrays, self.attrs )

    def resample( self, resampling: str ) -> "EDASDataset":
        if resampling is None: return self
        arrays = OrderedDict()
        for vid,array in self.arrayMap.items(): arrays[vid] = array.resample( resampling )
        return EDASDataset( arrays, self.attrs )

    def requiresSubset(self, target_domain: str ) -> bool:
        return len( self.domains.difference( { target_domain } ) ) > 0

    def getExtremeVariable(self, ext: Extremity ) -> EDASArray:
        sizes = [ x.size for x in self.inputs ]
        exVal = max( sizes ) if ext == Extremity.HIGHEST else min( sizes )
        return self.inputs[ sizes.index( exVal ) ]

    def getAlignmentVariable(self, alignRes: str ):
        return self.getExtremeVariable( Extremity.parse(alignRes) )

    def align( self, target_var: EDASArray ) -> "EDASDataset":
      new_vars = OrderedDict()
      for var in self.inputs: new_vars[var.name] = var.align(target_var)
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
            xrplot.plot( array.subset( domain, { domain.name } ).xr )

    def mplot(self, facet_axis: str = "time" ):
        for array in self.inputs:
            xrplot.plot( array.xr, col = facet_axis )

    def __iadd__(self, other: "EDASDataset" ) -> "EDASDataset":
        self.arrayMap.update( other.arrayMap )
        self.attrs.update( other.attrs )
        return self

    def plot(self, idmatch: str = None ):
        nplots = len( self.ids )
        fig, axes = plt.subplots( ncols=nplots )
        self.logger.info( "Plotting {} plot(s)".format(nplots) )
        if nplots == 1:
            self.xarrays[0].plot(ax=axes)
        else:
            xarrays = self.xarrays if idmatch is None else self.find_arrays(idmatch)
            for iaxis, result in enumerate( xarrays ):
                result.plot(ax=axes[iaxis])
        plt.show()

    def plotMap(self, index = 0, view = "geo" ):
        if view.lower().startswith("geo"):
            ax = plt.axes( projection=ccrs.PlateCarree() )
        elif view.lower().startswith("polar"):
            ax = plt.axes( projection=ccrs.NorthPolarStereo( ) )
        elif view.lower().startswith("epolar"):
            ax = plt.axes(projection=ccrs.AzimuthalEquidistant( -80, 90 ) )
        elif view.lower().startswith("mol"):
            ax = plt.axes(projection=ccrs.Mollweide())
        elif view.lower().startswith("rob"):
            ax = plt.axes(projection=ccrs.Robinson())
        else:
            raise Exception( "Unrecognized map view: " + view )

        self.xarrays[index].plot.contourf( ax=ax, levels=8, cmap='jet', robust=True, transform=ccrs.PlateCarree() )
        ax.coastlines()
        plt.show()

    def segment_modes(self) ->  List[xa.DataArray]:
        mode_arrays = []
        for xarray in self.xarrays:
            if "mode" in xarray.dims:
                for imode in range( xarray.shape[xarray.get_axis_num("mode")] ):
                   mode_arrays.append( xarray.isel( { "mode": slice(imode,imode+1) }).squeeze("mode") )
            else:
                mode_arrays.append(xarray)
        return mode_arrays

    def filterArraysByType(self, arrays: List[xa.DataArray], mtype: int ) ->  List[xa.DataArray]:
        plot_arrays = []
        for xarray in arrays:
            if "time" in xarray.dims:
                if( mtype == PlotType.PC ):
                    plot_arrays.append( xarray )
            elif( mtype == PlotType.EOF ):
                plot_arrays.append( xarray )
        return plot_arrays

    def plotMaps( self, nrows=2, view = "geo", mtype = PlotType.EOF ):
        plot_arrays = self.filterArraysByType( self.segment_modes(), mtype )
        nPlots = len(plot_arrays)
        if nPlots == 1:
            self.plotMap(0,view)
        else:
            nCols = math.ceil( nPlots/2 )
            if view.lower().startswith("geo"):
                fig, axes = plt.subplots( nrows=nrows, ncols=nCols, subplot_kw={ "projection": ccrs.PlateCarree() } )
            elif view.lower().startswith("polar"):
                fig, axes = plt.subplots( nrows=nrows, ncols=nCols, subplot_kw={ "projection": ccrs.NorthPolarStereo() } )
            elif view.lower().startswith("epolar"):
                fig, axes = plt.subplots( nrows=nrows, ncols=nCols, subplot_kw={ "projection": ccrs.AzimuthalEquidistant( -80, 90 ) } )
            elif view.lower().startswith("mol"):
                fig, axes = plt.subplots( nrows=nrows, ncols=nCols, subplot_kw={ "projection": ccrs.Mollweide() } )
            elif view.lower().startswith("rob"):
                fig, axes = plt.subplots( nrows=nrows, ncols=nCols, subplot_kw={ "projection": ccrs.Robinson() } )
            else:
                raise Exception( "Unrecognized map view: " + view )

            self.logger.info( "Plotting {} maps with nCols = {}".format( nPlots, nCols ) )

            for iaxis, xarray in enumerate(plot_arrays):
                icol, irow = iaxis%nCols, math.floor(iaxis/nCols)
                try:
                    ax = axes[irow,icol] if hasattr(axes, '__getitem__') else axes
                    xarray.plot.contourf( ax=ax, levels=8, cmap='jet', robust=True, transform=ccrs.PlateCarree() )
                    ax.set_title(xarray.name)
                    ax.coastlines()
                except Exception as err:
                    self.logger.error( "ERROR Plotting ( irow: {}, icol: {} ) of {} plots: {}".format(irow,icol,len(plot_arrays),str(err)))
            plt.show()
        while True: time.sleep(0.5)

    @classmethod
    def mergeArrayMaps( cls, amap0: Dict[str,EDASArray], amap1: Dict[str,EDASArray] )-> Dict[str,EDASArray]:
        result: Dict[str, EDASArray] = {}
        for key in amap0.keys():
            if (key in amap1) and ( id(amap0[key]) != id(amap1[key]) ):
                raise Exception( "Attempt to add different arrays with the same key to a Dataset: " + key )
            result[ key  ] = amap0[key]
        for key in amap1.keys():
            if key not in amap0:
                result[key] = amap1[key]
        return result

    @classmethod
    def merge(cls, dsets: List["EDASDataset"]):
        if len( dsets ) == 1: return dsets[0]
        arrayMap: OrderedDict[str,EDASArray] = OrderedDict()
        attrs: Dict[str,Any] = {}
        for dset in dsets:
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
        id =  self.attrs.get( "archive", "" )
        if not id: id = "archive-" + self.randomStr(4)
        return Archive.getFilePath( proj, exp, id )

    def parm(self, key: str, default: str) -> Any: return self.attrs.get(key,default)
    def __getitem__( self, key: str ) -> Any: return self.attrs.get( key )
    def __setitem__(self, key: str, value: Any ):
        if isinstance( value, EDASArray ): self.addArray( key, value, {} )
        else: self.attrs[key] = value

class MergeHandler:
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def mergeResults(self, results: List[EDASDataset] ) -> EDASDataset: pass

class StandardMergeHandler(MergeHandler):

    def mergeResults(self, results: List[EDASDataset] ) -> EDASDataset:
        return EDASDataset.merge( results )

class EDASDatasetCollection:

    def __init__( self, name: str = None, dsets: "OrderedDict[str, EDASDataset]" = None  ):
        from edask.process.task import Job
        self._name = Job.randomStr(6) if name is None else name
        self._datasets: OrderedDict[str, EDASDataset] = OrderedDict() if dsets is None else dsets
        print( " $$$$ DsetCol(" + self._name + ").init: " + self.arrayIds)

    @property
    def keys(self) -> KeysView[str]: return self._datasets.keys()

    def items(self) -> ItemsView[(str, EDASDataset)]: return self._datasets.items()

    def __getitem__( self, key: str ) -> EDASDataset: return self._datasets.get(key)

    def __setitem__(self, key: str, dset: EDASDataset ):
        assert isinstance(dset,EDASDataset), "EDASDatasetCollection.setitem: Expecting EDASDataset, got " + dset.__class__.__name__
        current = self._datasets.get(key, None)
        init_arrayIds = self.arrayIds
        self._datasets[key] = dset if current is None else EDASDataset.merge([current, dset])
        print( " $$$$ DsetCol(" + self._name + ").setitem[ " + key + "] <- " + dset.id + ": " + init_arrayIds + " -> " + self.arrayIds)

    @property
    def arrayIds(self): return "[ " + ", ".join([dsid+":"+key for dsid,dset in self._datasets.items() for key in dset.arrayMap.keys()]) + " ]"

    def __iadd__( self, other: "EDASDatasetCollection" ):
        for key in other.keys: self[key] = other[key]
        return self

    @property
    def attrs(self) -> Dict[str,str]:
        return { k:v for dset in self._datasets.values() for k,v in dset.attrs.items() }

    @property
    def arrays(self) -> List[EDASArray]:
        return [array for dset in self._datasets.values() for array in dset.arrayMap.values()]

    @property
    def dataset(self) -> EDASDataset:
        return EDASDataset.merge( list( self._datasets.values() ) )

    def filterByOperation( self, op: WorkflowNode ) -> "EDASDatasetCollection":
        filteredInputDatasets = EDASDatasetCollection(self._name + "-FilterByOperation")
        print(" %%%% DsetCol(" + self._name + "): PROCESSING connectors ")
        for connector in op.connectors:
            print(" %%%% DsetCol(" + self._name + "): PROCESS connector : " + connector.output)
            for vid in connector.inputs:
                print(" %%%% DsetCol(" + self._name + "): PROCESS connector input: " + vid)
                filteredInputDatasets[vid] = self[vid]
        print( " $$$$ DsetCol(" + self._name + "): filterByOperation[ " + op.name + "]: " + self.arrayIds + " -> " + filteredInputDatasets.arrayIds)
        return filteredInputDatasets

    def filterByConnector(self, inputConnector: OperationConnector ) -> "EDASDatasetCollection":
        filteredDatasets = EDASDatasetCollection(self._name + "-FilterByConnector")
        for vid in inputConnector.inputs: filteredDatasets[vid] = self[vid]
        return filteredDatasets

    def getResultDataset(self)-> EDASDataset:
        return EDASDataset.merge([dset.standardize( {"product": id} ) for id,dset in self._datasets.items()]).persist()

    def getExtremeVariable(self, ext: Extremity ) -> EDASArray:
        arrayList = self.arrays
        sizes = [ x.size for x in arrayList ]
        exVal = max( sizes ) if ext == Extremity.HIGHEST else min( sizes )
        return arrayList[ sizes.index( exVal ) ]

    def getAlignmentVariable(self, alignRes: str ):
        return self.getExtremeVariable( Extremity.parse(alignRes) )

    def align( self, alignRes: str ) -> "EDASDatasetCollection":
        tvar = self.getAlignmentVariable( alignRes )
        return EDASDatasetCollection( self._name + "-Align", OrderedDict( [ (key, dset.align(tvar)) for key,dset in self._datasets.items() ] ) )

    def groupby( self, grouping: str ) -> "EDASDatasetCollection":
        if grouping is None: return self
        return EDASDatasetCollection( self._name + "-Groupby", OrderedDict( [ (key, dset.groupby(grouping)) for key,dset in self._datasets.items() ] ) )

    def resample( self, resampling: str ) -> "EDASDatasetCollection":
        if resampling is None: return self
        return EDASDatasetCollection( self._name + "-Resample", OrderedDict( [ (key, dset.resample(resampling)) for key,dset in self._datasets.items() ] ) )