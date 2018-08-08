from ..kernel import Kernel, KernelSpec
import xarray as xr
from ..task import Task
import numpy as np
import numpy.ma as ma
import time, traceback
from xarray.ufuncs import cos

class AverageKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def buildWorkflow(self, input_dataset: xr.Dataset, task: Task ) -> xr.Dataset:
        variables = task.getMappedVariables( input_dataset )
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( task.inputs ) + ", task metadata = " + str(task.metadata) + ", axes = " + str(task.axes) )
        result_names = []
        for variable in variables:
            weights: xr.DataArray = cos( variable.coords.get( "y" ) ) if task.hasAxis('y') else None
            resultName = "-".join( [task.rId, variable.name] )
            result_names.append( resultName )
            input_dataset[ resultName ] = self.ave( variable, task.axes, weights )
        input_dataset.attrs[ "results-" + task.rId ] = result_names
        return input_dataset

    def ave( self, variable, axes, weights ) -> xr.DataArray:
        if weights is None:
            return variable.mean(axes)
        else:
            weighted_var = variable * weights
            sum = weighted_var.sum( axes )
            axes.remove("y")
            norm = weights * variable.count( axes ) if len( axes ) else weights
            return sum / norm.sum("y")



