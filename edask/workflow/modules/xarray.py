from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel, DualOpKernel, TimeOpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, OpNode
from edask.process.task import TaskRequest
from edask.workflow.data import EDASArray
from edask.process.node import Param, Node
from edask.collections.agg import Archive
from typing import List, Optional, Dict, Any
from  scipy import stats, signal
from edask.process.domain import Axis, DomainManager
from edask.data.cache import EDASKCacheMgr
from eofs.xarray import Eof
import datetime, os
import numpy as np


def accum( accumulator: xa.Dataset, array: xa.Dataset) -> xa.Dataset:
    return array if accumulator is None else accumulator + array

def weights( array: xa.Dataset ) -> xa.Dataset:
    return xa.ones_like( array ).where( array.notnull(), 0  )

class AverageKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str]  ) -> List[EDASArray]:
        return [ variable.ave(node.axes) ]

class MaxKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("max", "Maximum Kernel","Computes the maximum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        return [variable.max( node.axes )]

class MinKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("min", "Minimum Kernel","Computes the minimum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        return [variable.min( node.axes )]

class MeanKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("mean", "Mean Kernel","Computes the unweighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        return [variable.mean( node.axes )]

class MedianKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("median", "Median Kernel","Computes the median of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        return [variable.median( node.axes )]

class StdKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("mean", "Standard Deviation Kernel",
                "Computes the standard deviation of the array elements "
                "along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray,
                         attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        return [variable.std( node.axes )]

class NormKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("norm", "Normalization Kernel","Normalizes input arrays by centering (computing anomaly) and then dividing by the standard deviation along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        variable.persist()
        centered_result =  variable - variable.ave( node.axes )
        rv = [centered_result / centered_result.std( node.axes )]
        return rv

class FilterKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("filter", "Filter Kernel","Filters input arrays, currently only supports subsetting by month(s)" ) )
        self.requiredOptions.append("sel.*")

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        selection = node.findParm("sel.*")
        assert ( len(node.axes) == 0 ) or ( ( len(node.axes) == 1 ) and (node.axes[0] == 't') ), "Filter currently can only operate on the time axis"
        result = variable.filter( Axis.T, selection )
        return [ result ]

class DecycleKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("decycle", "Decycle Kernel","Removes the seasonal cycle from the temporal dynamics" ) )
        self.removeRequiredOptions(["ax.s"])

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        data = variable.persist()
        norm = bool(node.getParm("norm", False))
        grouping = node.getParm("groupby", 't.month')
        climatology = data.groupby(grouping).mean('t')
        anomalies = data.groupby(grouping) - climatology
        if norm:
            anomalies = anomalies.groupby(grouping) / data.groupby(grouping).std('t')
        return [variable.updateXa( anomalies, "decycle" )]

class DetrendKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("detrend", "Detrend Kernel","Detrends input arrays by "
                            "('method'='highapss'): subtracting the result of applying a 1D convolution (lowpass) filter along the given axes, "
                            "or ('method'='linear'): linear detrend over 'nbreaks' evenly spaced segments." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        data = variable.persist()
        axisIndex = variable.getAxisIndex( node.axes, 0, 0 )
        dim = data.dims[axisIndex]
        method = node.getParm("method", "highpass")
        if method == "highpass":
            window_size = node.getParm("wsize", data.shape[axisIndex]//8 )
            detrend_args = { dim:window_size, "center":True, "min_periods": 1 }
            trend = data.rolling(**detrend_args).mean()
            detrend: EDASArray = variable - variable.updateXa( trend, "trend" )
            return [detrend]
        elif method == "linear":
            nbreaks = node.getParm("nbreaks", 0 )
            data = variable.nd
            segSize = data.shape[0]/(nbreaks + 1.0)
            breaks = [ round(ix*segSize) for ix in range(nbreaks) ]
            detrended_data = signal.detrend( data, axis=axisIndex, bp=breaks )
            return [ variable.updateNp( detrended_data ) ]

class TeleconnectionKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("telemap", "Teleconnection Kernel",
                            "Produces teleconnection map by computing covariances at each point "
                            "(in roi) with location specified by 'lat' and 'lon' parameters." ) )
        self.removeRequiredOptions(["ax.s"])

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray,
                         attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        variable.persist()
        parms = self.getParameters( node, [ Param("lat"), Param("lon")])
        aIndex = variable.xr.get_axis_num('t')
        center: xa.DataArray = variable.selectPoint( float(parms["lat"]), float(parms["lon"]) ).xr
        cmean, data_mean = center.mean(axis=aIndex), variable.xr.mean(axis=aIndex)
        cstd, data_std = center.std(axis=aIndex), variable.xr.std(axis=aIndex)
        cov = np.sum((variable.xr - data_mean) * (center - cmean), axis=aIndex) / variable.xr.shape[aIndex]
        cor = cov / (cstd * data_std)
        return [ EDASArray( variable.name, variable.domId, cor ) ]

class LowpassKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("lowpass", "Lowpass Kernel","Smooths the input arrays by applying a 1D convolution (lowpass) filter along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        variable.persist()
        axisIndex = variable.getAxisIndex( node.axes, 0, 0 )
        dim = variable.xr.dims[axisIndex]
        window_size = node.getParm("wsize", variable.xr.shape[axisIndex]//8 )
        lowpass_args = { dim:window_size, "center":True, "min_periods": 1 }
        lowpass = variable.xr.rolling(**lowpass_args).mean()
        return [lowpass]

class EofKernel(TimeOpKernel):
    def __init__( self ):
        TimeOpKernel.__init__( self, KernelSpec("eof", "Eof Kernel","Computes PCs and EOFs along the time axis." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        variable.persist()
        nModes = node.getParm("modes", 16 )
        center = bool( node.getParm("center", "false") )
        input = variable.xr.rename( {"t":"time"} )
        solver = Eof( input, center = center )
        results = []
        if (len(products) == 0) or ( "eofs" in products):
            results.append( variable.updateXa( solver.eofs( neofs=nModes ), "eofs", { "mode": "m" }, "eofs" ) )
        if (len(products) == 0) or ( "pcs" in products):
            results.append( variable.updateXa( solver.pcs( npcs=nModes ).rename( {"time":"t"} ).transpose(), "pcs", { "mode": "m" }, "pcs"  ) )
        fracs = solver.varianceFraction( neigs=nModes )
        pves = [ str(round(float(frac*100.),1)) + '%' for frac in fracs ]
        for result in results: result["pves"] = str(pves)
        return results

class AnomalyKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("anomaly", "Anomaly Kernel", "Centers the input arrays by subtracting off the mean along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        variable.persist()
        return  [variable - variable.ave( node.axes )]

class VarKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("var", "Variance Kernel","Computes the variance of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        return [variable.var( node.axes )]

class SumKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("sum", "Sum Kernel","Computes the sum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        return [variable.sum( node.axes )]

class DiffKernel(DualOpKernel):
    def __init__( self ):
        DualOpKernel.__init__( self, KernelSpec("diff", "Difference Kernel","Computes the point-by-point differences of pairs of arrays." ) )

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset, products: List[str] ) -> EDASDataset:
        inputVars: List[EDASArray] = inputDset.inputs
        result = inputVars[0] - inputVars[1]
        return EDASDataset.init( { result.name: result }, inputDset.attrs )

class SubsetKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("subset", "Subset Kernel","NoOp kernel used to return (subsetted) inputs." ) )

    def processInputCrossSection(self, request: TaskRequest, node: OpNode, inputDset: EDASDataset, products: List[str]) -> EDASDataset:
        return inputDset

class NoOp(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("noop", "NoOp Kernel","NoOp kernel used to output intermediate products in workflow." ) )

    def processInputCrossSection(self, request: TaskRequest, node: OpNode, inputDset: EDASDataset, products: List[str]) -> EDASDataset:
        return inputDset

class CacheKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("cache", "Cache Kernel","Cache kernel used to cache input rois for low latency access by subsequest requests ." ) )
        self._maxInputs = 1

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str,Any], products: List[str] ) -> List[EDASArray]:
        cacheId = node.getParm( "result" )
        EDASKCacheMgr.cache( cacheId, variable )
        return [ variable ]


