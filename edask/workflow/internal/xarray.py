from ..kernel import Kernel, KernelSpec
import xarray as xr
from edask.process.operation import WorkflowNode, SourceInput
from edask.process.source import SourceType
from typing import List, Dict, Sequence, BinaryIO, TextIO, ValuesView
from edask.agg import Collection
import numpy as np
import numpy.ma as ma
import time, traceback
from xarray.ufuncs import cos
from edask.process.source import VariableSource, DataSource

class InputKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("input", "Data Input","Data input and workflow source node" ) )

    def buildWorkflow(self, op: WorkflowNode, input_dataset: xr.Dataset) -> xr.Dataset:
        result_datasets = [ input_dataset ] if input_dataset is not None else []
        varSource: VariableSource = op.source
        stype = varSource.source.type
        if stype == SourceType.collection:
            collection = Collection.new( varSource.source.address )
            aggs = collection.sortVarsByAgg(varSource.vids)
            for ( aggId, vars ) in aggs.items():
                result_datasets.append( xr.open_mfdataset( collection.pathList(aggId), autoclose=True, data_vars=vars, parallel=True) )
        else:
            if stype in [ SourceType.file, SourceType.dap ]:
                result_datasets.append( xr.open_mfdataset(varSource.source.address, autoclose=True, data_vars=varSource.getNames(), parallel=True) )
        return xr.merge( result_datasets )

class AverageKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def buildWorkflow(self, op: WorkflowNode, input_dataset: xr.Dataset) -> xr.Dataset:
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( op.inputs ) + ", op metadata = " + str(op.metadata) + ", axes = " + str(op.axes) )
        result_names = []
        for input in op.inputs:
            inop: WorkflowNode = input.getConnection()
            for vid in inop.source.getIds():
                variable = input_dataset[vid]
                weights: xr.DataArray = cos( variable.coords.get( "y" ) ) if op.hasAxis('y') else None
                resultName = "-".join( [op.rid, variable.name] )
                result_names.append( resultName )
                input_dataset[ resultName ] = self.ave( variable, op.axes, weights )
        input_dataset.attrs[ "results-" + op.rid ] = result_names
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



