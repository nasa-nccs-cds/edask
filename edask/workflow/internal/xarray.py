from ..kernel import Kernel, KernelSpec, KernelResult, OpKernel
import xarray as xr
from edask.process.operation import WorkflowNode, SourceNode, OpNode
from edask.process.task import TaskRequest
from typing import List, Dict, Optional


import numpy as np
import xarray as xr

class AverageKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        weights = self.getWeights( node, variable )
        if weights is None:
            return variable.mean(node.axes)
        else:
            axes = list(node.axes)
            weighted_var = variable * weights
            sum = weighted_var.sum( axes )
            axes.remove("y")
            norm = weights * variable.count( axes ) if len( axes ) else weights
            rv =  sum / norm.sum("y")
            return rv

    def getWeights(self, op: OpNode, variable: xr.DataArray  ) -> Optional[xr.DataArray]:
        if op.hasAxis('y'):
            ycoordaxis =  variable.coords.get( "y" )
            assert ycoordaxis is not None, "Can't identify Y coordinate axis, axes = " + str( variable.coords.keys() )
            return np.cos( ycoordaxis * (3.1415926536/180.0) )
        else: return None


class MaxKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("max", "Maximum Kernel","Computes the maximum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        return variable.max( dim=node.axes, keep_attrs=True )

class MinKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("min", "Minimum Kernel","Computes the minimum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        return variable.min( dim=node.axes, keep_attrs=True )

class MeanKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Mean Kernel","Computes the unweighted average of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        return variable.mean( dim=node.axes, keep_attrs=True )

class MedianKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("median", "Median Kernel","Computes the median of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        return variable.median( dim=node.axes, keep_attrs=True )

class StdKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("mean", "Standard Deviation Kernel","Computes the standard deviation of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        return variable.std( dim=node.axes, keep_attrs=True )

class VarKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("var", "Variance Kernel","Computes the variance of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        return variable.var( dim=node.axes, keep_attrs=True )

class SumKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("sum", "Sum Kernel","Computes the sum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        return variable.sum( dim=node.axes, keep_attrs=True )

class DiffKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("sum", "Sum Kernel","Computes the sum of the array elements along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: xr.DataArray ) -> xr.DataArray:
        return variable.sum( dim=node.axes, keep_attrs=True )


class SubsetKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("subset", "Subset Kernel","NoOp kernel used to return (subsetted) inputs." ) )

    def buildWorkflow( self, request: TaskRequest, wnode: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, NoOp inputs: " + str( [ str(w) for w in op.inputs ] ) )
        return KernelResult.merge(inputs)


class NoOp(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("noop", "NoOp Kernel","NoOp kernel used to output intermediate products in workflow." ) )

    def buildWorkflow( self, request: TaskRequest, wnode: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, NoOp inputs: " + str( [ str(w) for w in op.inputs ] ) )
        return KernelResult.merge(inputs)
