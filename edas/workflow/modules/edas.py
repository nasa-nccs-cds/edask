from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel, TimeOpKernel
import time, xarray as xa
from xarray.core.groupby import DataArrayGroupBy
from edas.process.operation import WorkflowNode, OpNode
from edas.process.task import TaskRequest
from edas.workflow.data import EDASArray, EDASDatasetCollection
from edas.process.node import Param, Node
from edas.collection.agg import Archive
from typing import List, Optional, Dict, Union
from  scipy import stats, signal
from edas.process.domain import Axis, DomainManager
from edas.data.cache import EDASKCacheMgr
from eofs.xarray import Eof
from collections import OrderedDict
import numpy as np


def accum( accumulator: xa.Dataset, array: xa.Dataset) -> xa.Dataset:
    return array if accumulator is None else accumulator + array

def weights( array: xa.Dataset ) -> xa.Dataset:
    return xa.ones_like( array ).where( array.notnull(), 0  )

class AverageKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray  ) -> EDASArray:
        return variable.ave(node.axes)

class MaxKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("max", "Maximum Kernel","Computes the maximum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.max( node.axes )

class RegridKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("regrid", "Regrid Kernel","Regrids the array according to gridSpec, e.g. 'uniform~.25x.25' or 'gaussian~32' " ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        gridSpec = node.getParam( Param("gridder", True) )
        return variable.regrid( gridSpec )

class MinKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("min", "Minimum Kernel","Computes the minimum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.min( node.axes )

class MeanKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("mean", "Mean Kernel","Computes the unweighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.mean( node.axes )

class MedianKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("med", "Median Kernel","Computes the median of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.median( node.axes )

class StdKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("std", "Standard Deviation Kernel",
                "Computes the standard deviation of the array elements "
                "along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.std( node.axes )

class NormKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("norm", "Normalization Kernel","Normalizes input arrays by centering (computing anomaly) and then dividing by the standard deviation along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        variable.persist()
        centered_result =  variable - variable.ave( node.axes )
        return centered_result / centered_result.std( node.axes )

class FilterKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("filter", "Filter Kernel","Filters input arrays, currently only supports subsetting by month(s)" ) )
        self.requiredOptions.append("sel.*")

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        selection = node.findParm("sel.*")
        assert ( len(node.axes) == 0 ) or ( ( len(node.axes) == 1 ) and (node.axes[0] == 't') ), "Filter currently can only operate on the time axis"
        result = variable.filter( Axis.T, selection )
        return result

class DecycleKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("decycle", "Decycle Kernel","Removes the seasonal cycle from the temporal dynamics" ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        data = variable.persist()
        norm = bool(node.getParm("norm", False))
        grouping = node.getParm("groupby", 't.month')
        climatology = data.groupby(grouping).mean('t')
        anomalies = data.groupby(grouping) - climatology
        if norm:
            anomalies = anomalies.groupby(grouping) / data.groupby(grouping).std('t')
        return variable.updateXa( anomalies, "decycle" )

class TimeAggKernel(OpKernel):
    def __init__(self):
        OpKernel.__init__(self, KernelSpec("timeAgg", "Time Aggregation Kernel", "Aggregates data over time into requested period bins"))

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputs: EDASDataset  ) -> EDASDataset:
        resultArrays = [ result for inputArray in inputs.arrayMap.values() for result in self.processVariables( request, node, inputArray )  ]
        return self.buildProduct( inputs.id, request, node, resultArrays, inputs.attrs )

    def processVariables(self, request: TaskRequest, node: OpNode, variable: EDASArray) -> List[EDASArray]:
        variable.persist()
        period = node.getParm("period", 'month')
        operation = str(node.getParm("op", 'mean')).lower()
        return variable.timeAgg( period, operation)

class TimeResampleKernel(OpKernel):
    def __init__(self):
        OpKernel.__init__(self, KernelSpec("timeResample", "Time Resample Kernel", "Aggregates data over time into the requested timestep"))

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputs: EDASDataset  ) -> EDASDataset:
        resultArrays = [ result for inputArray in inputs.arrayMap.values() for result in self.processVariables( request, node, inputArray )  ]
        return self.buildProduct( inputs.id, request, node, resultArrays, inputs.attrs )

    def processVariables(self, request: TaskRequest, node: OpNode, variable: EDASArray) -> List[EDASArray]:
        variable.persist()
        freq = node.getParm("freq", 'month')
        operation = str(node.getParm("op", 'mean')).lower()
        return variable.timeResample( freq, operation )

class WorldClimKernel(OpKernel):
    def __init__(self, kid:str = "worldClim" ):
        OpKernel.__init__(self, KernelSpec(kid, "WorldClim Kernel", "Computes the 20 WorldClim fields"))
        self.start_time = None
        self.results: Dict[str,EDASArray]  = {}
        self.selectors: Dict[str,np.ndarray] = {}

    def toCelcius(self, tempVar: EDASArray ):
        Tunits: str = tempVar.xr.attrs.get("units",None)
        if Tunits is None:
            if tempVar.xr[0,0,0] > 150:
                return tempVar - 273.15
        elif Tunits.lower().startswith("k"):
            return tempVar - 273.15
        return tempVar

    def exeLog(self, index: Union[int,float] ):
        self.logger.info(f"Computing WorldClim Field {index}, elapsed = {(time.time()-self.start_time)/60.0} min")

    def print(self, title: str, results: List[EDASArray]):
        self.logger.info( f"\n\n {title}" )
        for result in results:
            result = result.xr.load()
            self.logger.info("\n ***** Result {}, shape = {}, data:".format(result.name, str(result.shape)))
            self.logger.info(result.data.tolist())

    def print_array(self, name: str, array: xa.DataArray ):
        self.logger.info("\n ***** Result {}: {}, shape = {}, data:".format(name, array.name, str(array.shape)))
        self.logger.info( str( array.values.tolist() ) )

    def stack(self, array: xa.DataArray) -> xa.DataArray:
        for d in ['y', 'x']:
            if d not in array.dims:
                array = array.expand_dims( d, -1 )
        return array.stack( z=['y', 'x'] )

    def addMonthCoord(self, targetVar: EDASArray, taxis: str ) -> xa.DataArray:
        tcoord = targetVar.xr.coords[taxis]
        dt = tcoord[1] - tcoord[0]
        month: xa.DataArray = ((tcoord - tcoord[0]) / dt)
        return targetVar.xr.assign_coords(m=month)

    def getMonthArray(self, targetVar: EDASArray, taxis: str ) -> np.ndarray:
        tlen = targetVar.xr.coords[taxis].size
        tdata: np.ndarray = np.arange( tlen ).reshape( tlen, 1, 1 )
#        result = xa.DataArray( data=tdata, dims=targetVar.dims )
        return tdata

    def getSelector( self, selectionArray: xa.DataArray, selOp: str, taxis: str) -> np.ndarray:
        selectorName = f"{selectionArray.name}.{selOp}"
        assert selectionArray.shape[0] >= 14, "Must have at least a full year of data with a 1-month extension at each end for rolling window computation"
        if selectorName not in self.selectors:
            lowpassSelector: xa.DataArray = selectionArray.rolling({taxis: 3}, min_periods=3, center=True).mean()
            if selOp == "max":
                xaSelectedMonth: xa.DataArray = lowpassSelector.argmax(taxis, keep_attrs=True)
            elif selOp == "min":
                xaSelectedMonth: xa.DataArray = lowpassSelector.argmin(taxis, keep_attrs=True)
            else:
                raise Exception("Unrecognized operation in getValueForSelectedQuarter: " + selOp)
            selectedMonth: np.ndarray = xaSelectedMonth.to_masked_array()
            self.selectors[selectorName] = selectedMonth
        else:
            selectedMonth = self.selectors[selectorName]
        return selectedMonth

    def getValueForSelectedQuarter(self, taxis: str, targetVar: "EDASArray", tvarOp: str, selectionVar: "EDASArray", selOp: str, name: str) -> "EDASArray":
        t0 = time.time()
        selectedMonth: np.ndarray = self.getSelector( selectionVar.xr, selOp, taxis )
        tlen = targetVar.xr.shape[0]
        tdata: np.ndarray = np.arange(tlen).reshape(tlen, 1, 1)
        mask = (np.abs((tdata - selectedMonth)) < 2)
        resultXarray: xa.DataArray = targetVar.xr.where(mask).sum(axis=0) if tvarOp == "sum" else targetVar.xr.where(mask).mean(axis=0)
        t2 = time.time()
        self.logger.info(f" getValueForSelectedQuarter, dims = {selectionVar.xr.dims}, time = {t2 - t0} sec")
        return  targetVar.updateXa(resultXarray.compute(), name)

    def setResult( self, key: str, value: EDASArray ):
        self.logger.info( f"Computed value for WorldClim field bio-{key}")
        self.results[key] = value

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputs: EDASDataset  ) -> EDASDataset:
        self.logger.info( f"Computing WorldClim fields for domains: { [str(d) for d in request.operationManager.domains.domains.values()] }" )
        version = node.getParm("version", "mean")
        self.results: Dict[str,EDASArray] = {}
        moistID = node.getParm("moist", "moist")
        moistVar = inputs.findArray(moistID)
        assert moistVar is not None, f"Can't locate moisture variable {moistID} in inputs: {inputs.ids}"

        tempID = node.getParm("temp","temp")
        tempVar: EDASArray = inputs.findArray(tempID)
        pscale = float( node.getParm( "pscale", 1.0 ) )
        if tempVar is None:
            taxis = "t"
            tempMaxID = node.getParm("maxTemp", "maxTemp")
            tempMinID = node.getParm("minTemp", "minTemp")
            Tmax: EDASArray = inputs.findArray(tempMaxID)
            Tmin: EDASArray = inputs.findArray(tempMinID)
            monthlyPrecip: EDASArray = moistVar
            assert Tmax is not None and Tmax is not None, f"Must specify the temperature input variables using either the '{tempID}' parameter (hourly) or the '{tempMaxID}','{tempMinID}' parameters (monthly)"
        else:
            taxis = "m"
            tempVar: EDASArray = inputs.findArray( tempID )
            assert tempVar is not None, f"Can't locate temperature variable {tempID} in inputs: {inputs.ids}"
            tempVar = self.toCelcius( tempVar )
            dailyTmaxmin = tempVar.timeResample("1D","max,min")
            Tmaxmin = dailyTmaxmin[0].timeAgg("month", "max,min")
            Tmax: EDASArray = Tmaxmin[0]
            Tmin: EDASArray = Tmaxmin[1]
            monthlyPrecip = moistVar.timeAgg("month", version)[0]

        results =  self.computeIndices( Tmax.compute(), Tmin.compute(), monthlyPrecip.compute(), version = version, pscale=pscale, taxis=taxis )
        return self.buildProduct(inputs.id, request, node, results, inputs.attrs)

    def computeIndices(self, Tmax: EDASArray, Tmin: EDASArray, monthlyPrecip: EDASArray, **kwargs ) ->  List[EDASArray]:
        version = kwargs.get('version',"mean")
        pscale = kwargs.get('pscale', 1.0 )
        taxis = kwargs.get('taxis', 't' )
        self.logger.info(f" --> version = {version}, pscale = {pscale}")
        if pscale != float(1.0): monthlyPrecip = monthlyPrecip * pscale
        Tave = ((Tmax+Tmin)/2.0).compute()
        TKave = Tave + 273.15
        Trange = (Tmax-Tmin)/2.0
        self.start_time = time.time()
        print( f" Tave shape = {Tave.xr.shape}" )
        print( f" monthlyPrecip shape = {monthlyPrecip.xr.shape}" )

#         self.logger.info( f"Tmax sample: {Tmax.xr.to_masked_array()[2,10:12,10:12]}")
#         self.logger.info( f"Tmin sample: {Tmin.xr.to_masked_array()[2,10:12,10:12]}")
#         self.logger.info( f"Trange sample: {Trange.xr.to_masked_array()[2,10:12,10:12]}")

        self.setResult( '1' ,  Tave.ave([taxis], name="bio1") )
        self.setResult( '2' ,  Trange.ave([taxis], name="bio2") )
        self.setResult( '4' ,  Tave.std([taxis], name="bio4") )
        self.setResult( '4a',  (( TKave.std([taxis],keep_attrs=True)*100 )/(self.results['1'] + 273.15)).rename("bio4a") )
        self.setResult( '5' ,  Tmax.max([taxis], name="bio5") )
        self.setResult( '6' ,  Tmin.min([taxis], name="bio6") )
        self.setResult( '7' ,  (self.results['5'] - self.results['6']).rename("bio7") )
        self.setResult( '8' ,  self.getValueForSelectedQuarter( taxis, Tave, "ave", monthlyPrecip, "max", "bio8" ) )
        self.setResult( '9' ,  self.getValueForSelectedQuarter( taxis, Tave, "ave", monthlyPrecip, "min", "bio9" ) )
        self.setResult( '3' ,  ( ( self.results['2']*100 )/ self.results['7'] ).rename("bio3") )
        self.setResult( '10' , self.getValueForSelectedQuarter( taxis, Tave, "ave", Tave, "max", "bio10" ) )
        self.setResult( '11' , self.getValueForSelectedQuarter( taxis, Tave, "ave", Tave, "min", "bio11" ) )
        self.setResult( '12' , monthlyPrecip.sum([taxis], name="bio12") )
        self.setResult( '13' , monthlyPrecip.max([taxis], name="bio13") )
        self.setResult( '14' , monthlyPrecip.min([taxis], name="bio14") )
        self.setResult( '15' , (( monthlyPrecip.std([taxis]) * 100 )/( (self.results['12']/12) + 1 )).rename("bio15") )
        self.setResult( '16' , self.getValueForSelectedQuarter( taxis, monthlyPrecip, "sum", monthlyPrecip, "max", "bio16") )
        self.setResult( '17' , self.getValueForSelectedQuarter( taxis, monthlyPrecip, "sum", monthlyPrecip, "min", "bio17") )
        self.setResult( '18' , self.getValueForSelectedQuarter( taxis, monthlyPrecip, "sum", Tave, "max", "bio18" ) )
        self.setResult( '19' , self.getValueForSelectedQuarter( taxis, monthlyPrecip, "sum", Tave, "min", "bio19" ) )

        results: List[EDASArray] = [ monthlyPrecip.updateXa( result.xr, "bio-"+index ) for index, result in self.results.items() ]
        self.logger.info( f"Completed WorldClim computation, elapsed = {(time.time()-self.start_time)/60.0} m")
        return results


class DetrendKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("detrend", "Detrend Kernel","Detrends input arrays by "
                            "('method'='highapss'): subtracting the result of applying a 1D convolution (lowpass) filter along the given axes, "
                            "or ('method'='linear'): linear detrend over 'nbreaks' evenly spaced segments." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        data = variable.persist()
        axisIndex = variable.getAxisIndex( node.axes, 0, 0 )
        dim = data.dims[axisIndex]
        window_size = node.getParm("wsize", data.shape[axisIndex] // 8)
        detrend_args = {dim: int(window_size), "center": True, "min_periods": 1}
        trend = data.rolling(**detrend_args).mean()
        detrend: EDASArray = variable - variable.updateXa(trend, "trend")
        return detrend

        # method = node.getParm("method", "highpass")
        # if method == "highpass":
        #     window_size = node.getParm("wsize", data.shape[axisIndex]//8 )
        #     detrend_args = { dim:int(window_size), "center":True, "min_periods": 1 }
        #     trend = data.rolling(**detrend_args).mean()
        #     detrend: EDASArray = variable - variable.updateXa( trend, "trend" )
        #     return [detrend]
        # elif method == "linear":
        #     nbreaks = node.getParm("nbreaks", 0 )
        #     data = variable.nd
        #     segSize = data.shape[0]/(nbreaks + 1.0)
        #     breaks = [ round(ix*segSize) for ix in range(nbreaks) ]
        #     detrended_data = signal.detrend( data, axis=axisIndex, bp=breaks )
        #     return [ variable.updateNp( detrended_data ) ]

class TeleconnectionKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("telemap", "Teleconnection Kernel",
                            "Produces teleconnection map by computing covariances at each point "
                            "(in roi) with location specified by 'lat' and 'lon' parameters." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        variable.persist()
        parms = self.getParameters( node, [ Param("lat"), Param("lon")])
        aIndex = variable.xr.get_axis_num('t')
        center: xa.DataArray = variable.selectPoint( float(parms["lon"]), float(parms["lat"]) ).xr
        cmean, data_mean = center.mean(axis=aIndex), variable.xr.mean(axis=aIndex)
        cstd, data_std = center.std(axis=aIndex), variable.xr.std(axis=aIndex)
        cov = np.sum((variable.xr - data_mean) * (center - cmean), axis=aIndex) / variable.xr.shape[aIndex]
        cor = cov / (cstd * data_std)
        return EDASArray( variable.name, variable.domId, cor )

class LowpassKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("lowpass", "Lowpass Kernel","Smooths the input arrays by applying a 1D convolution (lowpass) filter along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        variable.persist()
        axisIndex = variable.getAxisIndex( node.axes, 0, 0 )
        dim = variable.xr.dims[axisIndex]
        window_size = node.getParm("wsize", variable.xr.shape[axisIndex]//8 )
        lowpass_args = { dim:int(window_size), "center":True, "min_periods": 1 }
        lowpass = variable.xr.rolling(**lowpass_args).mean()
        return EDASArray( variable.name, variable.domId, lowpass )

class EofKernel(TimeOpKernel):
    def __init__( self ):
        TimeOpKernel.__init__( self, KernelSpec("eof", "Eof Kernel","Computes PCs and EOFs along the time axis." ) )
        self._requiresAlignment = True

    def get_cdms_variables( self, inputDset ):
        rv = []
        for  input in inputDset.inputs:
            tvar = input.xr.rename( {"t":"time"} ).to_cdms2()
            tvar.id = input.name
            tvar.standard_name = input.name
            tvar.long_name = input.name
            rv.append( tvar )
        return rv

    def get_input_array(self, inputDset: EDASDataset ):
        info = { 'shapes': [], 'slicers': [] }
        islice = 0
        xarrays: List[xa.DataArray] = [ input.purge( {"t":"time"} ).xr for input in inputDset.inputs ]

        for xarray in xarrays:
            info['shapes'].append( xarray.shape[1:] )
            channels = np.prod(xarray.shape[1:])
            info['slicers'].append(slice(islice, islice + channels))
            islice += channels

        stacked_arrays = [ xarray.stack( s=xarray.dims[1:] ) for xarray in xarrays ]
        merged = xa.concat( stacked_arrays, dim='s' ) if len( stacked_arrays ) > 1 else stacked_arrays[0]
        return merged, info

    def getResults(self, eof_modes, slicers, shapes ):
        results = [ eof_modes[dict(s=slicer)].unstack('s') for slicer, shape in zip(slicers, shapes)]
        return results

    def rename(self, data: xa.DataArray, rename_dict: Dict[str,str] ):
        for item in rename_dict.items():
            rn = {item[0]:item[1]}
            try:
                data = data.rename(rn)
            except:
                self.logger.warning( "Can't execute rename {} on {} with dims {}".format( str(item), data.name, str( data.dims ) ) )
        return data

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset ) -> EDASDataset:
        nModes = int( node.getParm("modes", 16) )
        center = bool(node.getParm("center", "false"))
        merged_input_data, info = self.get_input_array( inputDset )
        shapes = info['shapes']
        slicers = info['slicers']
        solver = Eof( merged_input_data, center=center )
        results = []
        for iMode, eofs_result in enumerate( solver.eofs( neofs=nModes ) ):
            for iVar, eofs_data in enumerate( self.getResults( eofs_result, slicers, shapes ) ):
                input = inputDset.inputs[iVar]
                results.append( EDASArray( "-".join( [ "eof-", str(iMode), input.name ]), input.domId, eofs_data  ) )
        pcs_result = solver.pcs( npcs=nModes )
        pcs = EDASArray( "pcs[" + inputDset.id + "]", inputDset.inputs[0].domId, EDASArray.cleanupCoords( pcs_result, { "mode": "m", "pc": "m" } ).transpose() )
        results.append( pcs )
        fracs = solver.varianceFraction( neigs=nModes )
        pves = [ str(round(float(frac*100.),1)) + '%' for frac in fracs ]
        for result in results: result["pves"] = str(pves)
        return EDASDataset.init(self.renameResults(results, node), inputDset.attrs)

class AnomalyKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("anomaly", "Anomaly Kernel", "Centers the input arrays by subtracting off the mean along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        variable.persist()
        return  variable - variable.ave( node.axes )

class VarKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("var", "Variance Kernel","Computes the variance of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.var( node.axes )

class SumKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("sum", "Sum Kernel","Computes the sum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.sum( node.axes )

class DiffKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__(self, KernelSpec("diff", "Difference Kernel", "Computes the point-by-point differences of pairs of arrays."))

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDataset: EDASDataset ) -> EDASDataset:
        inputVars: List[EDASArray] = inputDataset.arrays
        result = inputVars[0] - inputVars[1]
        return EDASDataset.init( OrderedDict([(result.name,result)]), inputDataset.attrs )

class SubsetKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("subset", "Subset Kernel","NoOp kernel used to return (subsetted) inputs." ) )

    def processInputCrossSection(self, request: TaskRequest, node: OpNode, inputDataset: EDASDataset ) -> EDASDataset:
        return inputDataset

class NoOp(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("noop", "NoOp Kernel","NoOp kernel used to output intermediate products in workflow." ) )

    def processInputCrossSection(self, request: TaskRequest, node: OpNode, inputDataset: EDASDataset ) -> EDASDataset:
        return inputDataset

class CacheKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("cache", "Cache Kernel","Cache kernel used to cache input rois for low latency access by subsequest requests ." ) )
        self._maxInputs = 1

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        cacheId = node.getParm( "result" )
        EDASKCacheMgr.cache( cacheId, variable )
        return variable


