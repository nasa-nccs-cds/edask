from abc import ABCMeta, abstractmethod
import logging, cdms2, time, os, socket
from edask.messageParser import mParse
from .task import Task
import xarray as xr


class KernelSpec:
    def __init__( self, name, title, description, **kwargs ):
        self._name = name
        self._title = title
        self._description = description
        self._options = kwargs

    def name(self): return self._name

    def __str__(self): return ";".join( [ self._name, self.getTitle(), self.getDescription(), str(self._options) ] )

    def getDescription(self): return self._description
    def getTitle(self): return self._title
    def summary(self): return ";".join( [ self._name, self.getTitle() ] )

class Kernel:

    __metaclass__ = ABCMeta

    def __init__( self, spec: KernelSpec ):
        self.logger = logging.getLogger()
        self._spec = spec

    def name(self): return self._spec.name()

    def getSpec(self): return self._spec
    def getCapabilities(self): return self._spec.summary()
    def describeProcess( self ): return str(self._spec)

    @abstractmethod
    def buildWorkflow(self, task: Task, dataset: xr.Dataset ) -> xr.Dataset: pass


class LegacyKernel:
    __metaclass__ = ABCMeta

    def __init__( self, spec ):
        self.logger = logging.getLogger()
        self._spec = spec
        self.cacheReturn = [ False, True ]

    def getListParm( self, mdata, key, default="" ):
        return mdata.get( key, default ).split(",")

    def name(self): return self._spec.name()

    def executeTask( self, task, inputs ):
        t0 = time.time()
        self.cacheReturn = [ mParse.s2b(s) for s in task.metadata.get( "cacheReturn", "ft" ) ]
        results = self.executeOperations( task, inputs )
        self.logger.info( " >> Executed {0} operation in time {1}, #results = {2}".format( self._spec.name(), (time.time()-t0), len(results) ) )
        return results

    def executeReduceOp( self, task, inputs ):
        kernel_input_ids = [ inputId.split('-')[0] for inputId in task.inputs ]
        self.logger.info( " >> Executed Reduce operation on inputs {0}, available arrays: {1} ".format( str(kernel_input_ids), str(inputs.keys()) ) )
        return [ self.reduce( inputs[kernel_input_ids[0]], inputs[kernel_input_ids[1]], task ) ]

    def reduce( self, input0, input1, metadata, rId ): raise Exception( "Parallelizable kernel with undefined reduce method operating on T axis")

    def executeOperations( self, task, inputs ):
        self.logger.info( "\n\n Execute Operations, inputs: " + str(inputs) )
        kernel_inputs = [ inputs.get( inputId.split('-')[0] ) for inputId in task.inputs ]
        if None in kernel_inputs: raise Exception( "ExecuteTask ERROR: required input {0} not available in task inputs: {1}".format( task.inputs, inputs.keys() ))
        return [  self.validExecuteOperation(task,input) for input in kernel_inputs ]

    def validExecuteOperation( self, task, input ):
        self.validate(input)
        return self.executeOperation(task,input)

    def validate( self, input ) :
        if( input.array is None ):
            raise Exception( " @@@ Missing data for input " + str(input.name) + " in Kernel " + str(self._spec) )

    def executeOperation( self, task, input ): raise Exception( "Attempt to execute Kernel with undefined executeOperation method")

    def getCapabilities(self): return self._spec
    def getCapabilitiesStr(self): return str(self._spec)

    def getAxes( self, metadata ):
        axes = metadata.get("axes")
        if axes is None: return None
        else: return tuple( [ int(item) for item in axes ] )

    def saveGridFile( self, resultId, variable, createGridFile ):
        outdir = os.path.dirname(variable.gridfile)
        outpath = os.path.join(outdir, resultId + ".nc")
        if createGridFile:
            try:
                newDataset = cdms2.createDataset(outpath)
                grid = variable.getGrid()
                if not (grid is None):
                    axes = variable.getAxisList()
                    axis_keys = [ axis.axis for axis in axes ]
                    self.logger.info(" @RRR@ Copying axes: {0}".format( str(axis_keys) ))
                    for axis in axes:
                        if axis.isTime():
                            pass
                        else:
                            self.logger.info( "  @RRR@ axis: {0}, startVal: {1}".format( axis.axis, axis[0] ) )
                            newDataset.copyAxis(axis)
                    newDataset.copyGrid(grid)
                    newDataset.close()
                    self.logger.info( " @RRR@ saved Grid file: {0}".format( outpath ) )
            except Exception as err:
                self.logger.info( "  @RRR@ Can't create grid file " + outpath + ": " + str(err) )
        return outpath

# class CDMSKernel(Kernel):
#
#     def createResult(self, rid, result_var, input, task, createGridFile ):
#         rv = None
#         result_var.id = result_var.id  + "-" + rid
#         gridFilePath = self.saveGridFile( result_var.id, result_var, createGridFile )
#         varmd = { "origin": input.origin, "worker": socket.gethostname() }
#         if( gridFilePath ): varmd["gridfile"] = gridFilePath
#         for key in varmd: result_var.createattribute( key, varmd[key] )
#         result_metadata = dict( input.metadata )
#         for mdata in [ task.metadata, varmd ]: result_metadata.update( mdata )
#         result = cdmsArray.createModifiedResult( task.rId, input.origin, result_metadata, result_var )
#         self.logger.info( " #RS# Creating result({0}), shape = {1}".format( result.id, str(result.shape) ))
#         if self.cacheReturn[0]: self.cached_results[ result.id ] = result
#         if self.cacheReturn[1]: rv = result
#         return rv

class InputMode:
    __metaclass__ = ABCMeta

    def __init__( self, mode, spec ):
        self._spec = spec
        self._mode = mode

    @abstractmethod
    def execute(self): pass

if __name__ == "__main__":
    metadata = { "axes": "13", "index": 0 }

    print( str(metadata) )

#     newDataset.close()
