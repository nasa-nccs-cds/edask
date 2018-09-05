from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, OpNode, MasterNode
from edask.process.task import TaskRequest
from edask.workflow.data import EDASArray
from typing import List, Optional
import numpy as np
from keras.models import Sequential, Model
from keras.layers import Dense, Activation
from keras.engine.base_layer import Layer
from keras.optimizers import SGD
from keras.callbacks import TensorBoard, History, Callback

class LayerKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("layer", "Layer Kernel","Represents a layer in a neural network." ) )
        self.parent = "keras.model"

class ModelKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("model", "Model Kernel","Represents a neural network model." ) )

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset ) -> EDASDataset:
        masterNode: MasterNode = node["master"]
        keras_model = Sequential()
        input_layers: List[OpNode] = masterNode.getInputProxies()
        assert len(input_layers) == 1, "Must have one and only one input layer to network, found {}".format( len(input_layers) )
        input_layer: Layer = self.getLayer( input_layers[0], True )
        keras_model.add( input_layer )
        return inputDset

    def getLayer(self, layerNode: OpNode, isInput: bool ) -> Layer:
        type = layerNode.getParm("type","dense")
        if type == "dense":
            return Dense( units=self.dim, **self.parms )
        else:
            raise Exception( "Unrecognized layer type: " + type )

    # def createSequentialModel( self ):
    #     # type: () -> Sequential
    #     model = Sequential()
    #     nInputs = self.inputs.getInputDimension()
    #
    #     if self.layers is not None:
    #         for iLayer, layer in enumerate(self.layers):
    #             kwargs = { "input_dim": nInputs } if iLayer == 0 else {}
    #             instance = layer.instance(**kwargs)
    #             self.layer_instances.append( instance )
    #             model.add( instance )
    #
    #     elif self.hidden is not None:
    #         nHidden = len(self.hidden)
    #         nOutputs = self.outputs.getOutputSize()
    #         for hIndex in range(nHidden):
    #             if hIndex == 0:
    #                 if self.eager:  model.add(Dense(units=self.hidden[hIndex], activation=self.activation, input_tensor=self.inputData))
    #                 else:           model.add(Dense(units=self.hidden[hIndex], activation=self.activation, input_dim=nInputs))
    #             else:
    #                 model.add(Dense(units=self.hidden[hIndex], activation=self.activation))
    #         output_layer = Dense(units=nOutputs) if nHidden else Dense(units=nOutputs, input_dim=nInputs)
    #         model.add( output_layer )
    #
    #     sgd = SGD( lr=self.lr, decay=self.decay, momentum=self.momentum, nesterov=self.nesterov )
    #     model.compile(loss=self.lossFunction, optimizer=sgd, metrics=['accuracy'])
    #     if self.weights is not None: model.set_weights(self.weights)
    #     return model






