from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, OpNode, MasterNode
from edask.process.task import TaskRequest
from edask.workflow.data import EDASArray
from typing import List, Optional, Tuple, Dict, Any
from operator import mul
from functools import reduce
import copy, sys, logging, random, numpy as np
from keras.models import Sequential, Model
from keras.layers import Dense, Activation
from keras.engine.base_layer import Layer
from edask.process.operation import WorkflowConnector
from edask.collections.agg import Archive
from keras.optimizers import SGD
from keras.callbacks import TensorBoard, History, Callback
from edask.workflow.learning import FitResult, PerformanceTracker, KerasModel


class LayerKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("layer", "Layer Kernel","Represents a layer in a neural network." ) )
        self.parent = "keras.network"

class NetworkKernel(OpKernel):
    # Parent of (proxy) LayerKernel

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("network", "Network Kernel","Represents a neural network architecture." ) )

    def getModel( self, masterNode: MasterNode ):
        keras_layers = masterNode["layers"]
        keras_model = Sequential()
        for keras_layer in keras_layers:
            keras_model.add( keras_layer )
        return keras_model

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset, products: List[str] ) -> EDASDataset:
        assert isinstance( node, MasterNode ), "Model kernel must be associated with a Master Node"
        masterNode: MasterNode = node
        layerNodes: List[OpNode] = masterNode.getInputProxies()
        assert len(layerNodes) == 1, "Must have one and only one input layer to network, found {}".format( len(layerNodes) )
        layers, orderedLayerNodes = KerasModel.getLayers( layerNodes[0], inputDset )
        masterNode["layers"] = layers
        masterNode["layerNodes"] = orderedLayerNodes
        return inputDset

class ModelKernel(OpKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("model", "Model Kernel","Represents a trained neural network." ) )

    def processVariable(self, request: TaskRequest, node: OpNode, variable: EDASArray, attrs: Dict[str, Any], products: List[str]) -> List[EDASArray]:
        modelPath = self.archivePath( "model" )
        modelData: EDASDataset = EDASDataset.open_dataset( modelPath )
        layersSpec = modelData["layers"]
        assert layersSpec, "Missing levels spec in model data"
        layerNodes = [ OpNode.deserialize(spec) for spec in layersSpec.split(";") ]
        layers = KerasModel.instantiateLayers( layerNodes )
        model = KerasModel.getModel( layers )
        weights = KerasModel.packWeights( "finalWts", modelData )
        model.set_weights( weights )
        input_size = weights[0].shape[0]
        input = KerasModel.getNetworkInput( node, variable, input_size )
        if not node.product: node["product"] = "prediction"
        return [ KerasModel.map( "predict", model, input ) ]

class TrainKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("train", "Train Kernel","Train a neural network model." ) )
        self.weights = None
        self.bestFitResult: FitResult = None
        self.tensorboard = TensorBoard( log_dir=Archive.getLogDir(), histogram_freq=0, write_graph=True )
        self.stop_condition = "minValTrain"

    def getModel(self, node: OpNode ) -> Tuple[MasterNode,Model]:
        input_connection: WorkflowConnector = node.inputs[0]
        master_node = input_connection.connection
        assert isinstance( master_node, MasterNode ), "Training Kernel is not connected to a network!"
        return master_node, KerasModel.getModel( master_node["layers"] )

    def buildLearningModel(self, node: MasterNode, model: Model ):
        optArgs = node.getParms( ["lr", "decay", "momentum", "nesterov" ] )
        sgd = SGD( **optArgs )
        model.compile(loss=node.getParm("loss","mse"), optimizer=sgd, metrics=['accuracy'])
        if self.weights is not None: model.set_weights(self.weights)

    def fitModel(self, master_node: MasterNode, train_node: OpNode, model: Model, inputDset: EDASDataset, performanceTracker: PerformanceTracker) -> FitResult:
        batchSize = master_node.getParm( "batchSize", 200 )
        nEpocs = master_node.getParm( "epochs", 600 )
        validation_fract = master_node.getParm( "valFraction", 0.2 )
        shuffle = master_node.getParm( "shuffle", False )
        inputData = KerasModel.getTrainingData( master_node, inputDset, 1 )
        targetData = KerasModel.getTargetData( train_node, inputDset, 1 )
        initial_weights = model.get_weights()
        history: History = model.fit( inputData[0].nd, targetData[0].nd, batch_size=batchSize, epochs=nEpocs, validation_split=validation_fract, shuffle=shuffle, callbacks=[self.tensorboard,performanceTracker], verbose=0 )
        return self.updateHistory( history, initial_weights, performanceTracker )

    def buildResultDataset(self, inputDset: EDASDataset, train_node: OpNode )-> EDASDataset:
        result = self.bestFitResult
        master_node, model = self.getModel(train_node)
        arrays = {}
        loss_coord = ( "steps", range(result.train_loss_history.shape[0]) )
        arrays["loss"] =     EDASArray( "loss" ,     None, xa.DataArray( result.train_loss_history, coords=( loss_coord, ) ), [] )
        arrays["val_loss"] = EDASArray( "val_loss" , None, xa.DataArray( result.val_loss_history,   coords=( loss_coord, ) ), [] )
        attrs = copy.deepcopy(inputDset.attrs)
        attrs["loss"] = result.train_loss
        attrs["val_loss"] = result.val_loss
        attrs["nEpocs"] = result.nEpocs
        attrs["nInstances"] = result.nInstances
        attrs["merge"] = "min:val_loss"
        attrs["layers"] = ";".join( [ op.serialize() for op in master_node["layerNodes"] ] )
        arrays.update( KerasModel.unpackWeights( "initWts", result.initial_weights ) )
        arrays.update( KerasModel.unpackWeights( "finalWts", result.final_weights ) )
        attrs["nlayers"] = int( len(result.final_weights)/2 )
        model.set_weights(result.final_weights)
        inputData = KerasModel.getTrainingData(master_node, inputDset, 1)
        targetData = KerasModel.getTargetData(train_node, inputDset, 1)
        arrays["prediction"] = KerasModel.map( "prediction", model, inputData[0] )
        arrays["target"] = targetData[0]
        rv = EDASDataset( arrays, attrs )
        return rv

    def updateHistory( self, history: History, initial_weights: List[np.ndarray], performanceTracker: PerformanceTracker ) -> FitResult:
        if performanceTracker.nEpoc > 0:
            if not self.bestFitResult or ( performanceTracker.minValLoss < self.bestFitResult.val_loss ):
                self.bestFitResult = FitResult.new( history, initial_weights, performanceTracker.getWeights(), performanceTracker.minTrainLoss, performanceTracker.minValLoss, performanceTracker.nEpoc )
        return self.bestFitResult

    def getHistoryDataArray(self, history: History, id: str, nEpochs: int, transforms = [] )-> EDASArray:
        data = xa.DataArray(history.history[id], coords=[ range( nEpochs ) ], dims=["epochs"])
        return EDASArray( id, None, data, transforms )

    def getWtsArray(self, array: np.ndarray, id: str, transforms = [] )-> EDASArray:
        coords = [ ('inputs',range(array.shape[0]) ), ('nodes',range(array.shape[1]) ) ] if array.ndim == 2 else [ ('nodes',range(array.shape[0]) ) ]
        return EDASArray( id, None, xa.DataArray( array, coords=coords ), transforms )

    def processInputCrossSection( self, request: TaskRequest, train_node: OpNode, inputDset: EDASDataset, products: List[str] ) -> EDASDataset:
        self.reseed()
        nIterations = train_node.getParm( "iterations", 1 )
        self.logger.info( "Executing fit-model {} times".format(nIterations) )
        val_loss_values = []
        for idx in range( nIterations ):
            performanceTracker = PerformanceTracker( self.stop_condition )
            master_node, model = self.getModel( train_node )
            self.buildLearningModel( master_node, model )
            self.fitModel( master_node, train_node, model, inputDset, performanceTracker )
            val_loss_values.append( performanceTracker.minValLoss )
        self.logger.info( "Worker training results: val losses = " + str(val_loss_values) )
        return self.buildResultDataset(inputDset, train_node)

    def reseed(self):
        seed = random.randint(0, 2 ** 32 - 2)
        np.random.seed(seed)