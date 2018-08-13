from ..kernel import Kernel, KernelSpec
import xarray as xr
from edask.process.operation import Operation, SourceInput, SourceType
from typing import List, Dict, Sequence, BinaryIO, TextIO, ValuesView
from edask.agg import Collection
import numpy as np
import numpy.ma as ma
import time, traceback
from xarray.ufuncs import cos

class InputKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("input", "Data Input","Data input and workflow source node" ) )

    def buildWorkflow(self, task: Operation, input_dataset: xr.Dataset) -> xr.Dataset:
        result_datasets = [ input_dataset ] if input_dataset is not None else []
        inputs: List[SourceInput] = task.inputs
        for input in inputs:
            stype = input.source.type
            if stype == SourceType.file:
                result_datasets.append( xr.open_mfdataset(input.source.address, autoclose=True, data_vars=task.inputs, parallel=True) )
            elif stype == SourceType.collection:
                collection = Collection.new( input.source.address )
                aggs = collection.sortVarsByAgg(task.inputs)
                for ( aggId, vars ) in aggs.items():
                    result_datasets.append( xr.open_mfdataset(collection.pathList(aggId), autoclose=True, data_vars=vars, parallel=True) )
            elif stype == SourceType.dap:
                result_datasets.append( xr.open_mfdataset( input.source.address, autoclose=True, data_vars=task.inputs, parallel=True) )

        return xr.merge( result_datasets )


class AverageKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def buildWorkflow(self, task: Operation, input_dataset: xr.Dataset) -> xr.Dataset:
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



