from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel, EnsOpKernel
import xarray as xr
from edask.process.operation import WorkflowNode, SourceNode, OpNode
from edask.process.task import TaskRequest
from edask.workflow.results import EDASArray
from typing import List, Dict, Optional, Tuple
from edask.process.domain import Domain, Axis
import numpy as np
import numpy.ma as ma
import xarray as xr
from xarray.core import ops

def accum( accumulator: xr.DataArray, array: xr.DataArray) -> xr.DataArray:
    return array if accumulator is None else accumulator + array

def weights( array: xr.DataArray ) -> xr.DataArray:
    return xr.ones_like( array ).where( array.notnull(), 0  )

class AverageKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, inputVar: EDASArray ) -> EDASArray:
        weights = self.getWeights( node, inputVar )
        data = inputVar.data
        if weights is None:
            return inputVar.mean(node.axes)
        else:
            axes = list(node.axes)
            weighted_var = data * weights
            sum = weighted_var.sum( axes )
            axes.remove("y")
            norm = weights * data.count( axes ) if len( axes ) else weights
            new_data =  sum / norm.sum("y")
            return inputVar.updateData( new_data )

    def getWeights(self, op: OpNode, variable: EDASArray  ) -> Optional[xr.DataArray]:
        if op.hasAxis( Axis.Y ):
            ycoordaxis =  variable.axis( Axis.Y )
            assert ycoordaxis is not None, "Can't identify Y coordinate axis, axes = " + str( variable.axes() )
            return np.cos( ycoordaxis * (3.1415926536/180.0) )
        else: return None


class MaxKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("max", "Maximum Kernel","Computes the maximum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.max( node.axes )

class MinKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("min", "Minimum Kernel","Computes the minimum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.min( node.axes )

class MeanKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Mean Kernel","Computes the unweighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.mean( node.axes )

class MedianKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("median", "Median Kernel","Computes the median of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.median( node.axes )

class StdKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Standard Deviation Kernel","Computes the standard deviation of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.std( node.axes )

class VarKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("var", "Variance Kernel","Computes the variance of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.var( node.axes )

class SumKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("sum", "Sum Kernel","Computes the sum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return variable.sum( node.axes )

class DiffKernel(EnsOpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("diff", "Difference Kernel","Computes the point-by-point differences of pairs of arrays." ) )

    def processVariables( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset ) -> EDASDataset:
        inputVars: List[EDASArray] = inputDset.inputs
        return EDASDataset.init( [ inputVars[0] - inputVars[1] ], inputDset.attrs )

class EnsAve(EnsOpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("eave", "Ensemble Average Kernel","Computes the point-by-point average of a set of variables." ) )

    def processEnsArray(self, request: TaskRequest, node: OpNode, ensDim: str, inputArray: xr.DataArray) -> xr.DataArray:
        return inputArray.mean( dim=ensDim, keep_attrs=True)


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
