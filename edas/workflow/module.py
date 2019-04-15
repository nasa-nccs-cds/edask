import sys, inspect, logging, os, traceback
from abc import ABCMeta, abstractmethod
from edas.workflow.kernel import Kernel, InputKernel, EDASDataset, EDASDatasetCollection
from os import listdir
from os.path import isfile, join, os
from edas.process.operation import WorkflowNode,  WorkflowConnector, MasterNode, OpNode
from edas.process.task import TaskRequest, Job
from edas.util.logging import EDASLogger
from typing import List, Dict, Callable, Set, Optional
import xarray as xa



class OperationModule:
    __metaclass__ = ABCMeta

    def __init__( self, name: str ):
        self.logger =  EDASLogger.getLogger()
        self._name = name

    def getName(self) -> str: return self._name

    def executeTask(self, task: WorkflowNode, inputs):
        self.logger.error( "Executing Unimplemented method on abstract base class: " + self.getName() )
        return []

    @abstractmethod
    def getCapabilitiesXml(self): pass

    @abstractmethod
    def getSerializationStr(self):  pass

    def serialize(self): return "!".join([self._name, "python", self.getSerializationStr()])

    @property
    def xml(self): return self.getCapabilitiesXml()

class KernelModule(OperationModule):

    def __init__( self, name, kernels: Dict[str,Callable[[],Kernel]] ):
        self.logger =  EDASLogger.getLogger()
        self._kernels: Dict[str,Callable[[str],Kernel]] = kernels
        self._instances: Dict[str,Kernel] = {}
        OperationModule.__init__( self, name )

    def clear(self, node: WorkflowNode ):
        try:    del self._instances[node.instanceId]
        except: pass

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
        return self.createKernel( node.op.lower(), node.instanceId )

    def createKernel(self, op: str, instanceName: str ) -> Kernel:
        instance = self._instances.get( instanceName, None )
        if instance is None:
            constructor = self._kernels.get( op )
            assert constructor is not None, "Unidentified Kernel: " + op
            instance = constructor()
            self._instances[instanceName] = instance
        return instance

    def getCapabilitiesXml(self): return '<module name="{}"> {} </module>'.format(self.getName(), " ".join([kernel().getCapabilitiesXml() for kernel in self._kernels.values()]))
    def getCapabilitiesJson(self): return dict(name=self.getName(), kernels=[kernel().getCapabilities() for kernel in self._kernels.values()])
    def getSerializationStr(self): return "~".join([ kernel().serialize() for kernel in self._kernels.values() ])

    def describeProcess( self, op ):
        kernel = self._kernels.get( op )
        return kernel.describeProcess()

class KernelManager:

    def __init__( self ):
        self.logger =  EDASLogger.getLogger()
        self.operation_modules: Dict[str,KernelModule] = {}
        self.build()

    def build(self):
        directory = os.path.dirname(os.path.abspath(__file__))
        internals_path = os.path.join( directory, "modules")
        allfiles = [ os.path.splitext(f) for f in listdir(internals_path) if ( isfile(join(internals_path, f)) ) ]
        modules = [ ftoks[0] for ftoks in allfiles if ( (ftoks[1] == ".py") and (ftoks[0] != "__init__") ) ]
        for module_name in modules:
            module_path = "edas.workflow.modules." + module_name
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

    def getModule(self, op: WorkflowNode) -> KernelModule:
        return self.operation_modules[ op.module ]

    def getKernel(self, node: WorkflowNode):
        if node.module.lower() in [ "cdspark", "sparkml", "sparksql" ]:
            raise Exception( "The EDAS server has been updated (the 'CDSpark' module is no longer active, use 'xarray' instead), your scripts need to be updated for compatibility, see: https://www.nccs.nasa.gov/services/Analytics")
        assert node.module in self.operation_modules.keys(), "Unknown Kernel Module: " + node.module
        module = self.operation_modules[ node.module ]
        return module.getKernel(node)

    def getMasterKernel(self, node: MasterNode):
        [ module, op ] = node.name.split(".")
        module = self.operation_modules[ module ]
        return module.createKernel( op )

    def getCapabilitiesJson( self, type: str = "kernel" ) -> Dict:
        from edas.collection.agg import Collection
        self.logger.info( " GetCapabilities --> type: " + type )
        if( type.lower().startswith("ker") or type.lower().startswith("op")  ):
            specs = dict( modules = [opMod.getCapabilitiesJson() for opMod in self.operation_modules.values()] )
            return specs
        else:
            return {}

    def getCapabilitiesXml(self, type: str) -> str:
        from edas.collection.agg import Collection
        if type == None: type = "kernels"
        self.logger.info( " GetCapabilities --> type: " + str(type) )
        if( type.lower().startswith("ker") ):
            specs = [opMod.getCapabilitiesXml() for opMod in self.operation_modules.values()]
            return '<modules> {} </modules>'.format( " ".join( specs ) )
        elif( type.lower().startswith("col") ):
            specs = Collection.getCollectionsList()
            return '<collection> {} </collection>'.format( " ".join( specs ) )
        elif (type.lower().startswith("var")):
            type_toks = type.split("|")
            collection = Collection.new( type_toks[1] )
            return collection.getVariableSpec( type_toks[2] )
        else:
            raise Exception( "Unknown capabilities type: " + type )

    def serialize(self) -> str:
        specs = [ opMod.serialize() for opMod in self.operation_modules.values() ]
        return "|".join( specs )

    def describeProcess(self, module: str, op: str ) -> str:
        module = self.operation_modules[ module ]
        return module.describeProcess( op )

    def getInputDatasets(self, request: TaskRequest, op: WorkflowNode ) -> EDASDatasetCollection:
        dsetColl = EDASDatasetCollection("GetInputDatasets")
        print(" %%%% PROCESSING inputs ")
        for inputNode in op.inputNodes:
            print(" %%%% ADD INPUT : " + inputNode.name )
            dsetColl += self.buildSubWorkflow(request, inputNode )
        print( " $$$$ getInputDatasets: " + op.name + " -> " + dsetColl.arrayIds)
        return dsetColl

    def buildSubWorkflow(self, request: TaskRequest, op: WorkflowNode ) -> EDASDatasetCollection:
        print( " %%%% BuildSubWorkflow: " + op.name )
        subWorkflowDatasets: EDASDatasetCollection = self.getInputDatasets( request, op ).filterByOperation( op )
        result: EDASDatasetCollection =  self.getKernel( op ).getResultDataset( request, op, subWorkflowDatasets )
        print( " $$$$ buildSubWorkflow[ " + op.name + "]: " + subWorkflowDatasets.arrayIds + " -> " + result.arrayIds)
        return result

    def buildRequest(self, request: TaskRequest ) -> List[EDASDataset]:
        request.linkWorkflow()
        resultOps: List[WorkflowNode] =  self.replaceProxyNodes( request.getResultOperations() )
        assert len(resultOps), "No result operations (i.e. without 'result' parameter) found"
        self.logger.info( "Build Request, resultOps = " + str( [ node.name for node in resultOps ] ))
        result = EDASDatasetCollection("BuildRequest")
        for op in resultOps: result += self.buildSubWorkflow( request, op )
        self.cleanup( request )
        return result.getResultDatasets()

    def cleanup(self, request: TaskRequest):
        ops: List[WorkflowNode] = request.getOperations()
        for op in ops:
            module: KernelModule = self.getModule( op )
            module.clear( op )

    def buildIndices( self, size: int ) -> xa.DataArray:
        return xa.DataArray( range(size), coords=[('node',range(size))] )

    def buildTask( self, job: Job ) -> List[EDASDataset]:
        try:
            self.logger.error("Worker-> BuildTask, index = " + str(job.workerIndex) )
            request: TaskRequest = TaskRequest.new( job )
            return self.buildRequest( request )
        except Exception as err:
            self.logger.error( "BuildTask Exception: " + str(err) + "\n" + traceback.format_exc() )
            raise err

    def testBuildTask( self, job: Job ) -> List[EDASDataset]:
        try:
            self.logger.error("Worker-> BuildTask, index = " + str(job.workerIndex) )
            request: TaskRequest = TaskRequest.new( job )
            return self.buildRequest( request )
        except Exception as err:
            self.logger.error( "testBuildTask Exception: " + str(err) + "\n" + traceback.format_exc()  )
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
                    currentMasterNode.addMasterOutputs(rootNode.outputNodes)
                    masterNodeList.add( currentMasterNode )
                currentMasterNode.addProxy(rootNode)
            for inpputNode in rootNode.inputNodes:
                self.createMasterNodes( inpputNode, masterNodeList, currentMasterNode )

    def replaceProxyNodes( self, resultOps: List[WorkflowNode] )-> List[WorkflowNode]:
        masterNodes: Set[MasterNode] = set()
        for node in resultOps: self.createMasterNodes( node, masterNodes )
        for masterNode in masterNodes: masterNode.spliceIntoWorkflow()
        return resultOps


edasOpManager = KernelManager()







