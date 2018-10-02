import sys, inspect, logging, os, traceback
from abc import ABCMeta, abstractmethod
from edask.workflow.kernel import Kernel, InputKernel, EDASDataset
from os import listdir
from os.path import isfile, join, os
from edask.process.operation import WorkflowNode,  WorkflowConnector, MasterNode, OpNode
from edask.process.task import TaskRequest, Job
from typing import List, Dict, Callable, Set, Optional
import xarray as xa



class OperationModule:
    __metaclass__ = ABCMeta

    def __init__( self, name: str ):
        self.logger =  logging.getLogger()
        self._name = name

    def getName(self) -> str: return self._name

    def executeTask(self, task: WorkflowNode, inputs):
        self.logger.error( "Executing Unimplemented method on abstract base class: " + self.getName() )
        return []

    @abstractmethod
    def getCapabilities(self): pass

    @abstractmethod
    def getCapabilitiesStr(self): pass

    def serialize(self): return "!".join( [self._name, "python", self.getCapabilitiesStr() ] )


class KernelModule(OperationModule):

    def __init__( self, name, kernels: Dict[str,Callable[[],Kernel]] ):
        self.logger =  logging.getLogger()
        self._kernels: Dict[str,Callable[[],Kernel]] = kernels
        OperationModule.__init__( self, name )

    def isLocal( self, obj )-> bool:
        return str(obj).split('\'')[1].split('.')[0] == "__main__"

    # def executeTask(self, task: WorkflowNode, inputs):
    #     kernel = self.getKernel( task )
    #     if( kernel is None ): raise Exception( "Unrecognized kernel.py key: "+ task.op.lower() +", registered kernels = " + ", ".join( self._kernels.keys() ) )
    #     self.logger.info( "Executing Kernel: " + kernel.name() )
    #     action = task.metadata.get("action","execute")
    #     if( action == "execute"): return kernel.executeTask(task, inputs)
    #     elif( action == "reduce"): return kernel.executeReduceOp(task, inputs)
    #     else: raise Exception( "Unrecognized kernel.py action: " + action )

    def getKernel(self, node: WorkflowNode):
        return self.createKernel( node.op.lower() )

    def createKernel(self, name ):
        constructor = self._kernels.get( name )
        assert constructor is not None, "Unidentified Kernel: " + name
        return constructor()

    def getCapabilities(self): return [ kernel().getCapabilities() for kernel in self._kernels.values() ]
    def getCapabilitiesStr(self): return "~".join([ kernel().getCapabilities() for kernel in self._kernels.values() ])

    def describeProcess( self, op ):
        kernel = self._kernels.get( op )
        return kernel.describeProcess()

class KernelManager:

    def __init__( self ):
        self.logger =  logging.getLogger()
        self.operation_modules: Dict[str,KernelModule] = {}
        self.build()

    def build(self):
        directory = os.path.dirname(os.path.abspath(__file__))
        internals_path = os.path.join( directory, "modules")
        allfiles = [ os.path.splitext(f) for f in listdir(internals_path) if ( isfile(join(internals_path, f)) ) ]
        modules = [ ftoks[0] for ftoks in allfiles if ( (ftoks[1] == ".py") and (ftoks[0] != "__init__") ) ]
        for module_name in modules:
            module_path = "edask.workflow.modules." + module_name
            module = __import__( module_path, globals(), locals(), ['*']  )
            kernels = { InputKernel().name.lower(): InputKernel }
            for clsname in dir(module):
                mod_cls = getattr( module, clsname)
                if( inspect.isclass(mod_cls) and (mod_cls.__module__ == module_path) ):
                    try:
                        if issubclass( mod_cls, Kernel ):
                            kernels[ mod_cls().name.lower() ] = mod_cls
                            self.logger.debug(  " ----------->> Adding Kernel Class: " + str( clsname ) )
                        else: self.logger.debug(  " xxxxxxx-->> Skipping non-Kernel Class: " + str( clsname ) )
                    except TypeError as err:
                        self.logger.debug( "Skipping improperly structured class: " + clsname + " -->> " + str(err) )
            if len(kernels) > 0:
                self.operation_modules[module_name] = KernelModule( module_name, kernels )
                self.logger.debug(  " ----------->> Adding Module: " + str( module_name ) )
            else: self.logger.debug(  " XXXXXXXX-->> Skipping Empty Module: " + str( module_name ) )

    def getModule(self, task: WorkflowNode) -> KernelModule:
        return self.operation_modules[ task.module ]

    def getKernel(self, node: WorkflowNode):
        module = self.operation_modules[ node.module ]
        return module.getKernel(node)

    def getMasterKernel(self, node: MasterNode):
        [ module, op ] = node.name.split(".")
        module = self.operation_modules[ module ]
        return module.createKernel( op )

    def getCapabilitiesStr(self) -> str:
        specs = [ opMod.serialize() for opMod in self.operation_modules.values() ]
        return "|".join( specs )

    def describeProcess(self, module: str, op: str ) -> str:
        module = self.operation_modules[ module ]
        return module.describeProcess( op )

    def buildSubWorkflow(self, request: TaskRequest, op: WorkflowNode, products: List[str] ) -> EDASDataset:
        inputDatasets: List[EDASDataset] = [ ]
        kernel = self.getKernel( op )
        for input in op.inputs:
            if isinstance(input, WorkflowConnector):
                inputDatasets.append(self.buildSubWorkflow(request, input.connection, input.products))
        return kernel.getResultDataset( request, op, inputDatasets, products )

    def buildRequest(self, request: TaskRequest ) -> EDASDataset:
        request.linkWorkflow()
        resultOps: List[WorkflowNode] =  self.replaceProxyNodes( request.getResultOperations() )
        assert len(resultOps), "No result operations (i.e. without 'result' parameter) found"
        self.logger.info( "Build Request, resultOps = " + str( [ node.name for node in resultOps ] ))
        result = EDASDataset.merge( [ self.buildSubWorkflow( request, op, [] ) for op in resultOps ] )
        return result

    def buildIndices( self, size: int ) -> xa.DataArray:
        return xa.DataArray( range(size), coords=[('node',range(size))] )

    def buildTask( self, job: Job ) -> EDASDataset:
        try:
            request: TaskRequest = TaskRequest.new( job )
            return self.buildRequest( request ).standardize()
        except Exception as err:
            self.logger.error( "BuildTask Exception: " + str(err) )
            self.logger.info( traceback.format_exc() )
            raise err

    # def buildTasks(self, job: Job ) -> EDASDataset:
    #     try:
    #         instances = xa.DataArray( range(job.iterations), coords=[('node',range(job.iterations))] )
    #         xa.apply_ufunc(instances, self.buildTask, job )
    #         request: TaskRequest = TaskRequest.new( job )
    #         return self.buildRequest( request )
    #     except Exception as err:
    #         self.logger.error( "BuildTask Exception: " + str(err) )
    #         self.logger.info( traceback.format_exc() )
    #         raise err

    def createMasterNodes(self, rootNode: WorkflowNode, masterNodeList: Set[MasterNode], currentMasterNode: Optional[MasterNode] = None ):
        if rootNode.proxyProcessed:
            if (currentMasterNode is not None) and (rootNode.masterNode is not None) and (rootNode.masterNode.name == currentMasterNode.name):
                rootNode.masterNode.absorb( currentMasterNode )
                masterNodeList.remove( currentMasterNode )
        else:
            kernel = self.getKernel( rootNode )
            if kernel.parent is None:
                if currentMasterNode is not None:
                    currentMasterNode.addMasterInput(rootNode)
                currentMasterNode = None
            else:
                if currentMasterNode is None:
                    currentMasterNode = MasterNode(kernel.parent)
                    currentMasterNode.addMasterOutputs(rootNode.outputs)
                    masterNodeList.add( currentMasterNode )
                currentMasterNode.addProxy(rootNode)
            for conn in rootNode.inputs:
                self.createMasterNodes( conn.connection, masterNodeList, currentMasterNode )

    def replaceProxyNodes( self, resultOps: List[WorkflowNode] )-> List[WorkflowNode]:
        masterNodes: Set[MasterNode] = set()
        for node in resultOps: self.createMasterNodes( node, masterNodes )
        for masterNode in masterNodes: masterNode.spliceIntoWorkflow()
        return resultOps


edasOpManager = KernelManager()







