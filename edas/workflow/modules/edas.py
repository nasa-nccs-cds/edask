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
        OpKernel.__init__(self, KernelSpec("TimeAgg", "Time Aggregation Kernel", "Aggregates data over time into requested periods"))

    def processVariable(self, request: TaskRequest, node: OpNode, variable: EDASArray) -> EDASArray:
        variable.persist()
        period = node.getParm("period", 'month')
        operation = str(node.getParm("op", 'mean')).lower()
        return variable.timeAgg( period, operation)

class WorldClimKernel(OpKernel):
    def __init__(self, kid:str = "worldClim" ):
        OpKernel.__init__(self, KernelSpec(kid, "WorldClim Kernel", "Computes the 20 WorldClim fields"))
        self.start_time = None

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

    def getValueForSelectedQuarter(self, targetVar: Optional["EDASArray"], selectionVar: "EDASArray", op: str, name: str ) -> "EDASArray":
#        self.logger.info( f" getValueForSelectedQuarter, dims = {selectionVar.xr.dims}")
        assert selectionVar.xrArray.shape[0] == 12, "Must have full year of data to compute WorldClim fields"
        selectionData: xa.DataArray = selectionVar.xr.chunk({'m':3})
        lowpassSelector: xa.DataArray = selectionData.rolling( m=3, min_periods=2, center=True ).mean()
        if op == "max":   selectedMonth: xa.DataArray = lowpassSelector.argmax( "m", keep_attrs=True )
        elif op == "min": selectedMonth: xa.DataArray = lowpassSelector.argmin( "m", keep_attrs=True )
        else: raise Exception( "Unrecognized operation in getValueForSelectedQuarter: " + op )
#        self.print_array( "selectedMonth", selectedMonth )

        if targetVar is None:
            target = self.stack( selectionVar.xr)
            resultXarray =  target.isel( m=self.stack( selectedMonth) )
            resultVar = selectionVar
        else:
            targetVars = []
            selectors = [ ( selectedMonth - 1 ) % 12, selectedMonth, (selectedMonth + 1) % 12 ]
            target: xa.DataArray = self.stack( targetVar.xr)
            for selector in selectors:
                stacked_selector  = self.stack( selector )
                self.logger.info( f"getValueForSelectedQuarter: Processing target var {target.name} with shape {target.shape}, op: {op}, selector shape: {stacked_selector.shape}")
                target.load(); stacked_selector.load()
                self.logger.info("XX...")
                targetVars.append( target.isel( m=stacked_selector ) )
                self.logger.info("ZZ...")
            resultXarray = ( targetVars[0] + targetVars[1] + targetVars[2] ) / 3
            resultVar = targetVar
        return resultVar.updateXa( resultXarray.unstack(), name )


    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputs: EDASDataset  ) -> EDASDataset:
        self.logger.info( f"Computing WorldClim fields for domains: { [str(d) for d in request.operationManager.domains.domains.values()] }" )
        resultArrays: Dict[str,EDASArray] = {  }
        tempID = node.getParm("temp","temp")
        version = node.getParm("version", "mean")
        assert tempID is not None, "Must specify name of the temperature input variable using the 'temp' parameter"
        moistID = node.getParm("moist","moist")
        assert moistID is not None, "Must specify name of the moisture input variable using  the 'moist' parameter"
        tempVar: EDASArray = inputs.findArray( tempID )
        assert tempVar is not None, f"Can't locate temperature variable {tempID} in inputs: {inputs.ids}"
        tempVar = self.toCelcius( tempVar )
        moistVar = inputs.findArray( moistID )
        assert moistVar is not None, f"Can't locate moisture variable {moistID} in inputs: {inputs.ids}"
        dailyTmax = tempVar.timeResample("D","max")
        dailyTmin = tempVar.timeResample("D","min")
        monthlyPrecip = moistVar.timeAgg( "month", version )
        Tmax: EDASArray = dailyTmax.timeAgg("month","max")
        Tmin: EDASArray = dailyTmin.timeAgg("month", "min")
        Tave = (Tmax+Tmin)/2
        TKave = Tave + 273.15
        Trange = (Tmax-Tmin)/2
        self.start_time = time.time()

        self.exeLog(1);   resultArrays['1']  = Tave.ave(["m"], name="bio1")
        self.exeLog(2);   resultArrays['2']  = Trange.ave(["m"], name="bio2")
        self.exeLog(4);   resultArrays['4']  = Tave.std(["m"], name="bio4")
        self.exeLog(4.5); resultArrays['4a'] = (( TKave.std(["m"],keep_attrs=True)*100 )/(resultArrays['1'] + 273.15)).rename("bio4a")
        self.exeLog(5);   resultArrays['5']  = Tmax.max(["m"], name="bio5")
        self.exeLog(6);   resultArrays['6']  = Tmin.min(["m"], name="bio6")
        self.exeLog(7);   resultArrays['7']  = (resultArrays['5'] - resultArrays['6']).rename("bio7")
        self.exeLog(8);   resultArrays['8']  = self.getValueForSelectedQuarter( Tave, monthlyPrecip, "max", "bio8" )
        self.exeLog(9);   resultArrays['9']  = self.getValueForSelectedQuarter( Tave, monthlyPrecip, "min", "bio9" )
        self.exeLog(3);   resultArrays['3']  = ( ( resultArrays['2']*100 )/ resultArrays['7'] ).rename("bio3")
        self.exeLog(10);  resultArrays['10'] = self.getValueForSelectedQuarter( Tave, Tave, "max", "bio10" )
        self.exeLog(11);  resultArrays['11'] = self.getValueForSelectedQuarter( Tave, Tave, "min", "bio11" )
        self.exeLog(12);  resultArrays['12'] = monthlyPrecip.sum(["m"], name="bio12")
        self.exeLog(13);  resultArrays['13'] = monthlyPrecip.max(["m"], name="bio13")
        self.exeLog(14);  resultArrays['14'] = monthlyPrecip.min(["m"], name="bio14")
        self.exeLog(15);  resultArrays['15'] = (( monthlyPrecip.std(["m"]) * 100 )/( (resultArrays['12']/12) + 1 )).rename("bio15")
        self.exeLog(16);  resultArrays['16'] = self.getValueForSelectedQuarter( None, monthlyPrecip, "max", "bio16")
        self.exeLog(17);  resultArrays['17'] = self.getValueForSelectedQuarter( None, monthlyPrecip, "min", "bio17")
        self.exeLog(18);  resultArrays['18'] = self.getValueForSelectedQuarter( monthlyPrecip, Tave, "max", "bio18" )
        self.exeLog(19);  resultArrays['19'] = self.getValueForSelectedQuarter( monthlyPrecip, Tave, "min", "bio19" )

        return self.buildProduct( inputs.id, request, node, list(resultArrays.values()), inputs.attrs )

class WorldClimTestKernel(WorldClimKernel):
    def __init__(self):
        WorldClimKernel.__init__(self,"worldClimTest")

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputs: EDASDataset  ) -> EDASDataset:
        resultArrays: Dict[str,EDASArray] = {}
        tempID = node.getParm("temp","temp")
        assert tempID is not None, "Must specify name of the temperature input variable using the 'temp' parameter"
        moistID = node.getParm("moist","moist")
        assert moistID is not None, "Must specify name of the moisture input variable using the 'moist' parameter"
        tempVar = inputs.findArray( tempID )
        assert tempVar is not None, f"Can't locate temperature variable {tempID} in inputs: {inputs.ids}"
        tempVar = self.toCelcius( tempVar )
        moistVar = inputs.findArray( moistID )
        assert moistVar is not None, f"Can't locate moisture variable {moistID} in inputs: {inputs.ids}"
        self.print( "Input Fields", [ tempVar, moistVar ] )

        dailyTmax = tempVar.timeResample("D","max")
        dailyTmin = tempVar.timeResample("D","min")
        self.print("Daily Max/Min:", [dailyTmax, dailyTmin])

        monthlyPrecip = moistVar.timeAgg("month","sum")
        Tmax: EDASArray = dailyTmax.timeAgg("month","max")
        Tmin: EDASArray = dailyTmin.timeAgg("month", "min")
        self.print("Monthly Precip:", [monthlyPrecip])
        Tave = (Tmax+Tmin)/2
        self.print("Monthly Temp Max/Min/Ave:", [Tmax, Tmin, Tave])
        resultArrays['8'] = self.getValueForSelectedQuarter( Tave, monthlyPrecip, "max", "bio8" )
        return self.buildProduct(inputs.id, request, node, list(resultArrays.values()), inputs.attrs)



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


