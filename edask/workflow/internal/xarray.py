from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, SourceNode, OpNode
from edask.process.task import TaskRequest
from edask.workflow.data import EDASArray
from typing import List, Dict, Optional, Tuple
from edask.process.domain import Domain, Axis
from edask.eofs.solver import Eof
from scipy import ndimage
import numpy as np
import numpy.ma as ma
import cdms2 as cdms
from cdutil.times import ANNUALCYCLE
from xarray.core import ops

def accum( accumulator: xa.Dataset, array: xa.Dataset) -> xa.Dataset:
    return array if accumulator is None else accumulator + array

def weights( array: xa.Dataset ) -> xa.Dataset:
    return xa.ones_like( array ).where( array.notnull(), 0  )

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
            return inputVar.updateXa(new_data)

    def getWeights(self, op: OpNode, variable: EDASArray  ) -> Optional[xa.Dataset]:
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

class NormKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("norm", "Normalization Kernel","Normalizes input arrays by centering (computing anomaly) and then dividing by the standard deviation along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        centered_result =  variable - variable.mean( node.axes )
        return centered_result / centered_result.std( node.axes )

class DecycleKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("decycle", "Decycle Kernel","Removes the seasonal cycle from the temporal dynamics" ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        climatology = variable.data.groupby('t.month').mean('t')
        anomalies = variable.data.groupby('t.month') - climatology
        return variable.updateXa( anomalies )

class DetrendKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("detrend", "Detrend Kernel","Detrends input arrays by subtracting the result of applying a 1D convolution (lowpass) filter along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        axisIndex = variable.getAxisIndex( node.axes, 0, 0 )
        dim = variable.data.dims[axisIndex]
        window_size = node.getParm("wsize", variable.data.shape[axisIndex]//5 )
        interp_na = bool( node.getParm("interp_na", "false") )
        xadata = variable.data.interpolate_na( dim=dim, method='linear' ) if interp_na else variable.data
        detrend_args = { dim:window_size, "center":True, "min_periods": 1 }
        trend = xadata.rolling(**detrend_args).mean()
        detrend: EDASArray = variable - variable.updateXa( trend )
        return detrend

class EofKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("eof", "Eof Kernel","Computes PCs and EOFs along the time axis." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        removeCycle = bool( node.getParm( "decycle", "false" ) )
        detrend = bool( node.getParm( "detrend", "false" ) )
        window_size = node.getParm("wsize", variable.data.shape[0]//5 )
        nModes = node.getParm("modes", 16 )
        cdms_var: cdms.tvariable.TransientVariable = variable.data.interpolate_na( dim="time", method='linear' ).to_cdms2()
        decycled_data = self.remove_cycle( cdms_var ) if removeCycle else cdms_var
        detrended_data = self.remove_trend(decycled_data,window_size) if detrend else decycled_data
        solver = Eof( detrended_data )
        eofs = solver.eofs( neofs=nModes )
        pcs = solver.pcs( npcs=nModes ).transpose()
        projected_pcs = solver.projectField(detrended_data,neofs=32).transpose()
        fracs = solver.varianceFraction( neigs=nModes )
        pves = [ str(round(float(frac*100.),1)) + '%' for frac in fracs ]

    def remove_cycle(self, variable: cdms.tvariable.TransientVariable ) -> cdms.tvariable.TransientVariable:
        decycle = ANNUALCYCLE.departures( variable )
        return decycle

    def remove_trend(self, cdms_var: cdms.tvariable.TransientVariable, window_size ) -> cdms.tvariable.TransientVariable:
        from scipy import ndimage
        trend = ndimage.convolve1d( cdms_var.data, np.ones((window_size,))/float(window_size), 0, None, "reflect" )
        detrend = cdms_var - trend
        return detrend


class AnomalyKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("anomaly", "Anomaly Kernel", "Centers the input arrays by subtracting off the mean along the given axes." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> EDASArray:
        return  variable - variable.mean( node.axes )

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
