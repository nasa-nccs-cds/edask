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
        axes: list[str] = task.metadata.get("axes",[])
        weights: xr.DataArray  = cos( input_dataset.coords['lat'] )
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( task.inputs ) + ", task metadata = " + str(task.metadata) + ", axes = " + str(axes) )
        for varName in task.varNames():
            variable = input_dataset[varName]
            resultName = "-".join( [task.rId, varName] )
            input_dataset[ resultName ] = self.ave( variable, axes, weights )
        return input_dataset

    def ave(self, variable, axes, weights ) -> xr.DataArray:
        if axes.count("y") > 0:
            weighted_var = variable * weights
            sum = weighted_var.sum( axes )
            axes.remove("y")
            norm = weights * variable.count( axes ) if len( axes ) else weights
            return sum / norm.sum("y")
        else:
            return variable.mean( axes )


