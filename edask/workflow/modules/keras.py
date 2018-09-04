from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, OpNode
from edask.process.task import TaskRequest
from edask.workflow.data import EDASArray
from typing import List, Optional
from edask.process.domain import Axis
from eofs.xarray import Eof
import numpy as np

class LayerKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("layer", "Layer Kernel","Represents a layer in a neural network." ) )
        self.parent = "keras.model"

class ModelKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("model", "Model Kernel","Represents a neural network model." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        pass






