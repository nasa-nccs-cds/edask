from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, OpNode
from edask.process.task import TaskRequest
from edask.workflow.data import EDASArray
from typing import List, Optional
from edask.process.domain import Axis
from eofs.xarray import Eof
import numpy as np

class KerasKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("eof", "Eof Kernel","Computes PCs and EOFs along the time axis." ) )

class LayerKernel(KerasKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("eof", "Eof Kernel","Computes PCs and EOFs along the time axis." ) )

    def processVariable( self, request: TaskRequest, node: OpNode, variable: EDASArray ) -> List[EDASArray]:
        nModes = node.getParm("modes", 16 )
        center = bool( node.getParm("center", "false") )
        input = variable.xr.rename( {"t":"time"} )
        solver = Eof( input, center = center )
        eofs = variable.updateXa( solver.eofs( neofs=nModes ), "eofs" )
        pcs = variable.updateXa( solver.pcs( npcs=nModes ).rename( {"time":"t"} ).transpose(), "pcs" )
        fracs = solver.varianceFraction( neigs=nModes )
        pves = [ str(round(float(frac*100.),1)) + '%' for frac in fracs ]
        eofs["pves"] = str(pves)
        return [ eofs, pcs ]