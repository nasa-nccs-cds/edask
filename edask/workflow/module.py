import sys, inspect, logging, os
from abc import ABCMeta, abstractmethod
from edask.workflow.kernel import Kernel
from os import listdir
from os.path import isfile, join, os
from edask.workflow.task import Task
from typing import List, Dict

class OperationModule:
    __metaclass__ = ABCMeta

    def __init__( self, name: str ):
        self.logger =  logging.getLogger()
        self._name = name

    def getName(self) -> str: return self._name

    def executeTask( self, task: Task, inputs ):
        self.logger.error( "Executing Unimplemented method on abstract base class: " + self.getName() )
        return []

    @abstractmethod
    def getCapabilities(self): pass

    @abstractmethod
    def getCapabilitiesStr(self): pass

    def serialize(self): return "!".join( [self._name, "python", self.getCapabilitiesStr() ] )


class KernelModule(OperationModule):

    def __init__( self, name, kernels ):
        self.logger =  logging.getLogger()
        self._kernels: Dict[str,Kernel] = {}
        for kernel in kernels: self._kernels[ kernel.name().lower() ] = kernel
        OperationModule.__init__( self, name )

    def isLocal( self, obj )-> bool:
        return str(obj).split('\'')[1].split('.')[0] == "__main__"

    def executeTask( self, task: Task, inputs ):
        kernel = self.getKernel( task )
        if( kernel is None ): raise Exception( "Unrecognized kernel.py key: "+ task.op.lower() +", registered kernels = " + ", ".join( self._kernels.keys() ) )
        self.logger.info( "Executing Kernel: " + kernel.name() )
        action = task.metadata.get("action","execute")
        if( action == "execute"): return kernel.executeTask(task, inputs)
        elif( action == "reduce"): return kernel.executeReduceOp(task, inputs)
        else: raise Exception( "Unrecognized kernel.py action: " + action )

    def getKernel( self, task: Task ):
        key = task.op.lower()
        return self._kernels.get( key )

    def getCapabilities(self): return [ kernel.getCapabilities() for kernel in self._kernels.values() ]
    def getCapabilitiesStr(self): return "~".join([ kernel.getCapabilities() for kernel in self._kernels.values() ])

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
        internals_path = os.path.join( directory, "internal")
        allfiles = [ os.path.splitext(f) for f in listdir(internals_path) if ( isfile(join(internals_path, f)) ) ]
        modules = [ ftoks[0] for ftoks in allfiles if ( (ftoks[1] == ".py") and (ftoks[0] != "__init__") ) ]
        for module_name in modules:
            module_path = "edask.workflow.internal." + module_name
            module = __import__( module_path, globals(), locals(), ['*']  )
            kernels = []
            for clsname in dir(module):
                mod_cls = getattr( module, clsname)
                if( inspect.isclass(mod_cls) and (mod_cls.__module__ == module_path) ):
                    try:
                        if issubclass( mod_cls, Kernel ):
                            kernel_instance = mod_cls();
                            kernels.append( kernel_instance )
                            self.logger.debug(  " ----------->> Adding Kernel Class: " + str( clsname ) )
                        else: self.logger.debug(  " xxxxxxx-->> Skipping non-Kernel Class: " + str( clsname ) )
                    except TypeError as err:
                        self.logger.debug( "Skipping improperly structured class: " + clsname + " -->> " + str(err) )
            if len(kernels) > 0:
                self.operation_modules[module_name] = KernelModule( module_name, kernels )
                self.logger.debug(  " ----------->> Adding Module: " + str( module_name ) )
            else: self.logger.debug(  " XXXXXXXX-->> Skipping Empty Module: " + str( module_name ) )

    def getModule(self, task: Task ) -> KernelModule:
        return self.operation_modules[ task.module ]

    def getKernel( self, task: Task ):
        module = self.operation_modules[ task.module ]
        return module.getKernel(task)

    def getCapabilitiesStr(self) -> str:
        specs = [ opMod.serialize() for opMod in self.operation_modules.values() ]
        return "|".join( specs )

    def describeProcess(self, module: str, op: str ) -> str:
        module = self.operation_modules[ module ]
        return module.describeProcess( op )

edasOpManager = KernelManager()







