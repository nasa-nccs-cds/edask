from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel
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
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, inputVar: EDASArray ) -> List[EDASArray]:
        weights = self.getWeights( node, inputVar )
        data = inputVar.xr
        if weights is None:
            return [inputVar.mean(node.axes)]
        else:
            axes = list(node.axes)
            weighted_var = data * weights
            sum = weighted_var.sum( axes )
            axes.remove("y")
            norm = weights * data.count( axes ) if len( axes ) else weights
            new_data =  sum / norm.sum("y")
            return [inputVar.updateXa(new_data,"ave")]

    def getWeights(self, op: OpNode, variable: EDASArray  ) -> Optional[xa.Dataset]:
        if op.hasAxis( Axis.Y ):
            ycoordaxis =  variable.axis( Axis.Y )
            assert ycoordaxis is not None, "Can't identify Y coordinate axis, axes = " + str( variable.axes() )
            return np.cos( ycoordaxis * (3.1415926536/180.0) )
        else: return None

class MaxKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("max", "Maximum Kernel","Computes the maximum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        return [variable.max( node.axes )]

class MinKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("min", "Minimum Kernel","Computes the minimum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        return [variable.min( node.axes )]

class MeanKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Mean Kernel","Computes the unweighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        return [variable.mean( node.axes )]

class MedianKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("median", "Median Kernel","Computes the median of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        return [variable.median( node.axes )]

class StdKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Standard Deviation Kernel","Computes the standard deviation of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        return [variable.std( node.axes )]

class NormKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("norm", "Normalization Kernel","Normalizes input arrays by centering (computing anomaly) and then dividing by the standard deviation along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        centered_result =  variable - variable.mean( node.axes )
        return [centered_result / centered_result.std( node.axes )]

class DecycleKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("decycle", "Decycle Kernel","Removes the seasonal cycle from the temporal dynamics" ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        norm = bool(node.getParm("norm", False))
        grouping = node.getParm("groupby", 't.month')
        climatology = variable.xr.groupby(grouping).mean('t')
        anomalies = variable.xr.groupby(grouping) - climatology
        if norm:
            anomalies = anomalies.groupby(grouping) / variable.xr.groupby(grouping).std('t')
        return [variable.updateXa( anomalies, "decycle" )]

# class DecycleKernel1(OpKernel):
#     def __init__( self ):
#         Kernel.__init__( self, KernelSpec("decycle1", "Decycle Kernel","Removes the seasonal cycle from the temporal dynamics" ) )
#
#     def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
#         norm = bool(node.getParm( "norm", False ) )
#         grouping = node.getParm("groupby", 't.month' )
#         climatology = variable.xr.groupby(grouping).mean('t')
#         anomalies = variable.xr.groupby(grouping) - climatology
#         if norm:
#             anomalies = anomalies.groupby(grouping) / variable.xr.groupby(grouping).std('t')
#         return [variable.updateXa( anomalies, "decycle" )]

class DetrendKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("detrend", "Detrend Kernel","Detrends input arrays by subtracting the result of applying a 1D convolution (lowpass) filter along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        axisIndex = variable.getAxisIndex( node.axes, 0, 0 )
        dim = variable.xr.dims[axisIndex]
        window_size = node.getParm("wsize", variable.xr.shape[axisIndex]//8 )
        detrend_args = { dim:window_size, "center":True, "min_periods": 1 }
        trend = variable.xr.rolling(**detrend_args).mean()
        detrend: EDASArray = variable - variable.updateXa( trend, "trend" )
        return [detrend]

class EofKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("eof", "Eof Kernel","Computes PCs and EOFs along the time axis." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        nModes = node.getParm("modes", 16 )
        center = bool( node.getParm("center", "false") )
        input = variable.xr.rename( {"t":"time"} )
        solver = Eof( input, center = center )
        eofs = variable.updateXa( solver.eofs( neofs=nModes ), "eofs" )
        pcs = variable.updateXa( solver.pcs( npcs=nModes ).rename( {"time":"t"} ).transpose(), "pcs" )
        fracs = solver.varianceFraction( neigs=nModes )
        pves = [ str(round(float(frac*100.),1)) + '%' for frac in fracs ]
        eofs["pves"] = str(pves)
        return [ eofs, pcs ]

class AnomalyKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("anomaly", "Anomaly Kernel", "Centers the input arrays by subtracting off the mean along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        return  [variable - variable.mean( node.axes )]

class VarKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("var", "Variance Kernel","Computes the variance of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        return [variable.var( node.axes )]

class SumKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("sum", "Sum Kernel","Computes the sum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        return [variable.sum( node.axes )]

class DiffKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("diff", "Difference Kernel","Computes the point-by-point differences of pairs of arrays." ) )
        self._minInputs = 2
        self._maxInputs = 2

    def processInputCrossSection(self, request: TaskRequest, node: OpNode, inputDset: EDASDataset) -> EDASDataset:
        inputVars: List[EDASArray] = inputDset.inputs
        results = [ inputVars[0] - inputVars[1] ]
        return EDASDataset.init( results, inputDset.attrs )

class SubsetKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("subset", "Subset Kernel","NoOp kernel used to return (subsetted) inputs." ) )

    def buildWorkflow(self, request: TaskRequest, wnode: WorkflowNode, inputs: List[EDASDataset]) -> EDASDataset:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, NoOp inputs: " + str( [ str(w) for w in op.inputs ] ) )
        return EDASDataset.merge(inputs)


class NoOp(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("noop", "NoOp Kernel","NoOp kernel used to output intermediate products in workflow." ) )

    def buildWorkflow(self, request: TaskRequest, wnode: WorkflowNode, inputs: List[EDASDataset]) -> EDASDataset:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, NoOp inputs: " + str( [ str(w) for w in op.inputs ] ) )
        return EDASDataset.merge(inputs)

class ArchiveKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("archive", "Archive Result Data","Save request result data onto cluster for use by subsequest requests" ) )

    def buildWorkflow(self, request: TaskRequest, wnode: WorkflowNode, inputs: List[EDASDataset]):
        node: OpNode = wnode
        project = node.getParm("proj", DomainManager.randomId(6) )
        experiment = node.getParm("exp", request.name  + "." + datetime.datetime.now().strftime("%m-%d-%y.%H-%M-%S") )
        resultPath = Archive.getExperimentPath( project, experiment )
        result: EDASDataset = EDASDataset.merge(inputs)
        renameMap = { id:id.split("[")[0] for id in result.ids }
        result.xr.rename(renameMap).to_netcdf( resultPath, mode="w" )
        self.logger.info( "Archived results {} to {}".format( result.id, resultPath ) )
        result["archive"] = resultPath
        return result

