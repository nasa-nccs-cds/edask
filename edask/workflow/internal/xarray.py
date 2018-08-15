from ..kernel import Kernel, KernelSpec, KernelResult
import xarray as xr
from edask.process.operation import WorkflowNode, SourceNode, OpNode
from edask.process.task import TaskRequest
from edask.process.source import SourceType
from edask.process.domain import Axis
from typing import List, Dict, Sequence, BinaryIO, TextIO, ValuesView
from edask.agg import Collection
from edask.process.source import VariableSource, DataSource
import numpy as np

class InputKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("input", "Data Input","Data input and workflow source node" ) )

    def buildWorkflow( self, request: TaskRequest, node: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        snode: SourceNode = node
        dataSource: DataSource = snode.varSource.dataSource
        result: KernelResult = KernelResult.empty()
        if dataSource.type == SourceType.collection:
            collection = Collection.new( dataSource.address )
            aggs = collection.sortVarsByAgg( snode.varSource.vids )
            for ( aggId, vars ) in aggs.items():
                dset = xr.open_mfdataset( collection.pathList(aggId), autoclose=True, data_vars=vars, parallel=True)
                coordMap = Axis.getDatasetCoordMap( dset )
                dset.rename( snode.varSource.name2id(coordMap), True )
                result.addResult( dset )
        else:
            if dataSource.type in [ SourceType.file, SourceType.dap ]:
                dset = xr.open_mfdataset(dataSource.address, autoclose=True, data_vars=snode.varSource.getIds(), parallel=True)
                coordMap = Axis.getDatasetCoordMap( dset )
                dset.rename( snode.varSource.name2id(coordMap), True )
                result.addResult( dset, snode.varSource.getIds() )
        return result

class AverageKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def buildWorkflow( self, request: TaskRequest, wnode: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( op.inputs ) + ", op metadata = " + str(op.metadata) + ", axes = " + str(op.axes) )
        result: KernelResult = KernelResult.empty()
        resultArray: xr.DataArray = None
        for kernelResult in inputs:
            for variable in kernelResult.getInputs():
                if op.hasAxis('y'):
                    ycoordaxis =  variable.coords.get( "y" )
                    weights: xr.DataArray = np.cos( ycoordaxis )
                    resultArray = self.ave( variable, op.axes, weights )
                else:
                    resultArray = self.ave( variable, op.axes )
                resultArray.name = "-".join( [ op.rid, variable.name] )
                result.addArray( resultArray, kernelResult.dataset.attrs )
        return result

    def ave( self, variable, axes, weights ) -> xr.DataArray:
        if weights is None:
            return variable.mean(axes)
        else:
            weighted_var = variable * weights
            sum = weighted_var.sum( axes )
            axes.remove("y")
            norm = weights * variable.count( axes ) if len( axes ) else weights
            return sum / norm.sum("y")



