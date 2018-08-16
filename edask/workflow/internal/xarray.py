from ..kernel import Kernel, KernelSpec, KernelResult
import xarray as xr
from edask.process.operation import WorkflowNode, SourceNode, OpNode
from edask.process.task import TaskRequest
from edask.process.source import SourceType
from edask.process.domain import Axis
from typing import List, Dict, Optional
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
                result.addResult( dset, snode.varSource.ids() )
        else:
            if dataSource.type in [ SourceType.file, SourceType.dap ]:
                dset = xr.open_mfdataset(dataSource.address, autoclose=True, data_vars=snode.varSource.ids(), parallel=True)
                coordMap = Axis.getDatasetCoordMap( dset )
                dset.rename( snode.varSource.name2id(coordMap), True )
                result.addResult(dset, snode.varSource.ids())
        return result

class AverageKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the area-weighted average of the array elements along the given axes." ) )

    def buildWorkflow( self, request: TaskRequest, wnode: WorkflowNode, inputs: List[KernelResult] ) -> KernelResult:
        op: OpNode = wnode
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Build Workflow, inputs: " + str( [ str(w) for w in op.inputs ] ) + ", op metadata = " + str(op.metadata) + ", axes = " + str(op.axes) )
        result: KernelResult = KernelResult.empty()
        for kernelResult in inputs:
            for variable in kernelResult.getInputs():
                resultArray = self.ave( variable, op.axes, self.getWeights( op, variable ) )
                resultArray.name = op.getResultId( variable.name )
                self.logger.info(" Process Input {} -> {}".format( variable.name, resultArray.name ))
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

    def getWeights(self, op: OpNode, variable: xr.DataArray  ) -> Optional[xr.DataArray]:
        if op.hasAxis('y'):
            ycoordaxis =  variable.coords.get( "y" )
            assert ycoordaxis is not None, "Can't identify Y coordinate axis, axes = " + str( variable.coords.keys() )
            return np.cos( ycoordaxis )
        else: return None


