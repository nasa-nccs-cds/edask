from ..kernel import Kernel, KernelSpec, EDASDataset, OpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, OpNode, MasterNode
from edask.process.task import TaskRequest
from edask.workflow.data import EDASArray
from typing import List, Optional, Tuple
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
from edask.workflow.learning import FitResult, PerformanceTracker

def notNone( x ): return x is not None

class LayerKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("layer", "Layer Kernel","Represents a layer in a neural network." ) )
        self.parent = "keras.model"

class ModelKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("model", "Model Kernel","Represents a neural network model." ) )

    # def processInputCrossSection1( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset, products: List[str] ) -> EDASDataset:
    #     assert isinstance( node, MasterNode ), "Model kernel must be associated with a Master Node"
    #     masterNode: MasterNode = node
    #     keras_model = Sequential()
    #     layerNodes: List[OpNode] = masterNode.getInputProxies()
    #     assert len(layerNodes) == 1, "Must have one and only one input layer to network, found {}".format( len(layerNodes) )
    #     input_layer: Layer = self.getLayer( layerNodes[0], inputDset )
    #     keras_model.add( input_layer )
    #     while True:
    #         layerNodes = layerNodes[0].outputs
    #         assert len(layerNodes) == 1, "Currently only support sequential networks (one node per layer), found {}".format( len(layerNodes) )
    #         current_layer: Layer = self.getLayer( layerNodes[0] )
    #         if current_layer is None: break
    #         keras_model.add( current_layer )
    #     masterNode["model"] = keras_model
    #     return inputDset

    def getModel( self, masterNode: MasterNode ):
        keras_layers = masterNode["layers"]
        keras_model = Sequential()
        for keras_layer in keras_layers:
            keras_model.add( keras_layer )
        return keras_model

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset, products: List[str] ) -> EDASDataset:
        assert isinstance( node, MasterNode ), "Model kernel must be associated with a Master Node"
        masterNode: MasterNode = node
        keras_layers = []
        layerNodes: List[OpNode] = masterNode.getInputProxies()
        assert len(layerNodes) == 1, "Must have one and only one input layer to network, found {}".format( len(layerNodes) )
        input_layer: Layer = self.getLayer( layerNodes[0], inputDset )
        keras_layers.append( input_layer )
        while True:
            layerNodes = layerNodes[0].outputs
            assert len(layerNodes) == 1, "Currently only support sequential networks (one node per layer), found {}".format( len(layerNodes) )
            current_layer: Layer = self.getLayer( layerNodes[0] )
            if current_layer is None: break
            keras_layers.append( current_layer )
        masterNode["layers"] = keras_layers
        return inputDset

    def getLayer(self, layerNode: OpNode, inputDset: Optional[EDASDataset] = None, **kwargs ) -> Optional[Layer]:
        if layerNode.name != "keras.layer": return None
        args = { **layerNode.getMetadata( ignore=["input", "result", "axis", "axes", "name"] ), **kwargs }
        type = args.get("type","dense")
        if inputDset is not None:
            axes = layerNode.axes
            assert axes, "Must use 'axis' parameter in first layer to specify input coordinate"
            sizes = [ inputDset.getCoord(coord_name).size for coord_name in axes ]
            args["input_dim"] = reduce(mul, sizes, 1)
        if type == "dense":
            return Dense( **args )
        else:
            raise Exception( "Unrecognized layer type: " + type )

class TrainKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("train", "Train Kernel","Train a neural network model." ) )
        self.weights = None
        self.bestFitResult: FitResult = None
        self.tensorboard = TensorBoard( log_dir=Archive.getLogDir(), histogram_freq=0, write_graph=True )
        self.stop_condition = "minValTrain"

    def getKerasModel( self, masterNode: MasterNode ) -> Model:
        keras_layers = masterNode["layers"]
        keras_model = Sequential()
        for keras_layer in keras_layers:
            keras_model.add( copy.deepcopy(keras_layer) )
        return keras_model

    def getModel(self, node: OpNode ) -> Tuple[MasterNode,Model]:
        input_connection: WorkflowConnector = node.inputs[0]
        master_node = input_connection.connection
        assert isinstance( master_node, MasterNode ), "Training Kernel is not connected to a network!"
        return master_node, self.getKerasModel(master_node)

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
        inputData = self.getTrainingData( master_node, inputDset, 1 )
        targetData = self.getTargetData( train_node, inputDset, 1 )
        initial_weights = model.get_weights()
        history: History = model.fit( inputData[0], targetData[0], batch_size=batchSize, epochs=nEpocs, validation_split=validation_fract, shuffle=shuffle, callbacks=[self.tensorboard,performanceTracker], verbose=0 )
        return self.updateHistory( history, initial_weights, performanceTracker )

    def buildResultDataset(self,inputDset: EDASDataset)-> EDASDataset:
        result = self.bestFitResult
        arrays = {}
        arrays["loss"] = self.getDataArray( result.train_loss_history, "loss", "epochs" )
        arrays["val_loss"] = self.getDataArray( result.val_loss_history, "val_loss", "epochs" )
        attrs = copy.deepcopy(inputDset.attrs)
        attrs["loss"] = result.train_loss
        attrs["val_loss"] = result.val_loss
        attrs["nEpocs"] = result.nEpocs
        attrs["nInstances"] = result.nInstances
        attrs["merge"] = "min:val_loss"
        rv = EDASDataset( arrays, attrs )
        return rv

 #       arrays = { id: self.getDataArray( history, id, nEpocs ) for id in [ "loss", "val_loss" ] }
 #       return EDASDataset( arrays, inputDset.attrs )

    def updateHistory( self, history: History, initial_weights: List[np.ndarray], performanceTracker: PerformanceTracker ) -> FitResult:
        if performanceTracker.nEpoc > 0:
            if not self.bestFitResult or ( performanceTracker.minValLoss < self.bestFitResult.val_loss ):
                self.bestFitResult = FitResult.new( history, initial_weights, performanceTracker.getWeights(), performanceTracker.minTrainLoss, performanceTracker.minValLoss, performanceTracker.nEpoc )
        return self.bestFitResult

    def getHistoryDataArray(self, history: History, id: str, nEpochs: int, transforms = [] )-> EDASArray:
        data = xa.DataArray(history.history[id], coords=[ range( nEpochs ) ], dims=["epochs"])
        return EDASArray( id, None, data, transforms )

    def getDataArray(self, array: np.ndarray, id: str, dim: str, transforms = [] )-> EDASArray:
        data = xa.DataArray( array, coords=[(dim,range(array.shape[0]))] )
        return EDASArray( id, None, data, transforms )

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
        return self.buildResultDataset(inputDset)

    def getTrainingData(self, model_node: WorkflowNode, inputDset: EDASDataset, required_size = None ) -> List[np.ndarray]:
        train_input_ids = [ inp.name for inp in model_node.inputs ]
        assert (required_size == None) or (len( train_input_ids ) == required_size), "Train Kernel expects exactly {} input(s): got {}".format( required_size, len( train_input_ids ) )
        return self.getInputData( train_input_ids, inputDset, model_node.axes[0], 1 )

    def getTargetData(self, train_node: WorkflowNode, inputDset: EDASDataset, required_size = None ) -> List[np.ndarray]:
        target_input_ids = train_node.getParm("target","").split(",")
        assert (required_size == None) or (len( target_input_ids ) == required_size), "Train Kernel expects exactly {} target(s): got {}".format( required_size, len( target_input_ids ) )
        return self.getInputData( target_input_ids, inputDset, train_node.axes[0], 0 )

    def getInputData( self, ids: List[str], inputDset: EDASDataset, dim: str, expectedDimIndex: int ) -> List[np.ndarray]:
        train_inputs = list( filter( notNone, [ inputDset.getArray(id) for id in ids ] ) )
        assert len( train_inputs ), "Can't find input data for training, looking for {}, found {}".format( ids, inputDset.ids )
        return self.getAlignedArrays( train_inputs, dim, expectedDimIndex )

    def getAlignedArrays( self, inputs: List[EDASArray], dim: str, expectedDimIndex: int ) -> List[np.ndarray]:
        results: List[np.ndarray] = []
        for array in inputs:
            try:
                dimIndex = array.dims.index( dim )
                results.append( array.xr.T.data if dimIndex != expectedDimIndex else array.xr.data )
            except ValueError: raise Exception( "Can't find dim {} in network input dimensions: {}".format(dim,array.dims) )
        return results

    def reseed(self):
        seed = random.randint(0, 2 ** 32 - 2)
        np.random.seed(seed)

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


