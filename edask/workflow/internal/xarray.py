from edask.workflow.kernel import Kernel, KernelSpec
import numpy as np
import numpy.ma as ma
import time, traceback

class AverageKernel(Kernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average Kernel","Computes the average of the array elements along the given axes.", reduceOp="sumw", postOp="normw", nOutputsPerInput=2 ) )

    def executeOperations(self, task, inputs):
        kernel_inputs = [inputs.get(inputId.split('-')[0]) for inputId in task.inputs]
        if None in kernel_inputs: raise Exception( "ExecuteTask ERROR: required input {0} not available in task inputs: {1}".format(task.inputs, inputs.keys()))
        results = []
        axes = self.getAxes(task.metadata)
        self.logger.info("  ~~~~~~~~~~~~~~~~~~~~~~~~~~ Execute Operations, inputs: " + str( task.inputs ) + ", task metadata = " + str(task.metadata) + ", axes = " + str(axes) )
        for input in kernel_inputs:
            t0 = time.time()
            if( input.array is None ): raise Exception( "Missing data for input " + input.name + " in Average Kernel" )
            result_array = input.array.sum( axis=axes,   keepdims=True )
            mask_array = input.array.count(axis=self.getAxes(task.metadata), keepdims=True )
            results.append( npArray.createResult( task, input, result_array.filled( input.array.fill_value )  ) )
            results.append( npArray.createAuxResult( task.rId + "_WEIGHTS_", dict( input.metadata, **task.metadata ), input, mask_array  ) )
            t1 = time.time()
            self.logger.info( " ------------------------------- SUMW KERNEL: Operating on input '{0}', shape = {1}, origin = {2}, result sample = {3}, undef = {4}, time = {5}".format( input.name, input.shape, input.origin, result_array.flat[0:10], input.array.fill_value, t1-t0 ))
        return results
