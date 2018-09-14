from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel, DualOpKernel, TimeOpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, OpNode
from edask.process.task import TaskRequest
from edask.workflow.data import EDASArray
from edask.collections.agg import Archive
from typing import List, Optional
from edask.process.domain import Axis, DomainManager
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

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str]  ) -> List[EDASArray]:
        return [ variable.ave(node.axes) ]

class MaxKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("max", "Maximum Kernel","Computes the maximum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        return [variable.max( node.axes )]

class MinKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("min", "Minimum Kernel","Computes the minimum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        return [variable.min( node.axes )]

class MeanKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("mean", "Mean Kernel","Computes the unweighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        return [variable.mean( node.axes )]

class MedianKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("median", "Median Kernel","Computes the median of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        return [variable.median( node.axes )]

class StdKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("mean", "Standard Deviation Kernel","Computes the standard deviation of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        return [variable.std( node.axes )]

class NormKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("norm", "Normalization Kernel","Normalizes input arrays by centering (computing anomaly) and then dividing by the standard deviation along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        centered_result =  variable - variable.ave( node.axes )
        rv = [centered_result / centered_result.std( node.axes )]
        return rv

class FilterKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("filter", "Filter Kernel","Filters input arrays, currently only supports subsetting by month(s)" ) )
        self.requiredOptions.append("sel.*")

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        selection = node.findParm("sel.*")
        assert ( len(node.axes) == 0 ) or ( ( len(node.axes) == 1 ) and (node.axes[0] == 't') ), "Filter currently can only operate on the time axis"
        result = variable.filter( Axis.T, selection )
        return [ result ]

class DecycleKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("decycle", "Decycle Kernel","Removes the seasonal cycle from the temporal dynamics" ) )
        self.removeRequiredOptions(["ax.s"])

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        norm = bool(node.getParm("norm", False))
        grouping = node.getParm("groupby", 't.month')
        climatology = variable.xr.groupby(grouping).mean('t')
        anomalies = variable.xr.groupby(grouping) - climatology
        if norm:
            anomalies = anomalies.groupby(grouping) / variable.xr.groupby(grouping).std('t')
        return [variable.updateXa( anomalies, "decycle" )]

class DetrendKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("detrend", "Detrend Kernel","Detrends input arrays by subtracting the result of applying a 1D convolution (lowpass) filter along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        axisIndex = variable.getAxisIndex( node.axes, 0, 0 )
        dim = variable.xr.dims[axisIndex]
        window_size = node.getParm("wsize", variable.xr.shape[axisIndex]//8 )
        detrend_args = { dim:window_size, "center":True, "min_periods": 1 }
        trend = variable.xr.rolling(**detrend_args).mean()
        detrend: EDASArray = variable - variable.updateXa( trend, "trend" )
        return [detrend]

class LowpassKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("lowpass", "Lowpass Kernel","Smooths the input arrays by applying a 1D convolution (lowpass) filter along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        axisIndex = variable.getAxisIndex( node.axes, 0, 0 )
        dim = variable.xr.dims[axisIndex]
        window_size = node.getParm("wsize", variable.xr.shape[axisIndex]//8 )
        lowpass_args = { dim:window_size, "center":True, "min_periods": 1 }
        lowpass = variable.xr.rolling(**lowpass_args).mean()
        return [lowpass]

class EofKernel(TimeOpKernel):
    def __init__( self ):
        TimeOpKernel.__init__( self, KernelSpec("eof", "Eof Kernel","Computes PCs and EOFs along the time axis." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        nModes = node.getParm("modes", 16 )
        center = bool( node.getParm("center", "false") )
        input = variable.xr.rename( {"t":"time"} )
        solver = Eof( input, center = center )
        results = []
        if (len(products) == 0) or ( "eof" in products):
            results.append( variable.updateXa( solver.eofs( neofs=nModes ), "eof", { "mode": "m" }, "eof" ) )
        if (len(products) == 0) or ( "pc" in products):
            results.append( variable.updateXa( solver.pcs( npcs=nModes ).rename( {"time":"t"} ).transpose(), "pc", { "mode": "m" }, "pc"  ) )
        fracs = solver.varianceFraction( neigs=nModes )
        pves = [ str(round(float(frac*100.),1)) + '%' for frac in fracs ]
        for result in results: result["pves"] = str(pves)
        return results

class AnomalyKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("anomaly", "Anomaly Kernel", "Centers the input arrays by subtracting off the mean along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        return  [variable - variable.ave( node.axes )]

class VarKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("var", "Variance Kernel","Computes the variance of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
        return [variable.var( node.axes )]

class SumKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("sum", "Sum Kernel","Computes the sum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray, products: List[str] ) -> List[EDASArray]:
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

class ArchiveKernel(OpKernel):
    def __init__( self ):
        OpKernel.__init__( self, KernelSpec("archive", "Archive Result Data","Save request result data onto cluster for use by subsequest requests" ) )
        self.removeRequiredOptions(["ax.s"])

    def buildWorkflow(self, request: TaskRequest, wnode: WorkflowNode, inputs: List[EDASDataset], products: List[str]) -> EDASDataset:
        node: OpNode = wnode
        project = node.getParm("proj", DomainManager.randomId(6) )
        experiment = node.getParm("exp", request.name  + "." + datetime.datetime.now().strftime("%m-%d-%y.%H-%M-%S") )
        resultPath = Archive.getExperimentPath( project, experiment )
        result: EDASDataset = OpKernel.buildWorkflow( request, wnode, inputs, products )
        renameMap = { id:array.product for id,array in result.arrayMap.items() }
        result.xr.rename(renameMap).to_netcdf( resultPath, mode="w" )
        self.logger.info( "Archived results {} to {}".format( result.id, resultPath ) )
        result["archive"] = resultPath
        return result

