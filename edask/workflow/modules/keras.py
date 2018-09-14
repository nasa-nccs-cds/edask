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

def notNone( x ): return x is not None

class LayerKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("layer", "Layer Kernel","Represents a layer in a neural network." ) )
        self.parent = "keras.model"

class ModelKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("model", "Model Kernel","Represents a neural network model." ) )

    def processInputCrossSection( self, request: TaskRequest, node: OpNode, inputDset: EDASDataset, products: List[str] ) -> EDASDataset:
        assert isinstance( node, MasterNode ), "Model kernel must be associated with a Master Node"
        masterNode: MasterNode = node
        keras_model = Sequential()
        layerNodes: List[OpNode] = masterNode.getInputProxies()
        assert len(layerNodes) == 1, "Must have one and only one input layer to network, found {}".format( len(layerNodes) )
        input_layer: Layer = self.getLayer( layerNodes[0], inputDset )
        keras_model.add( input_layer )
        while True:
            layerNodes = layerNodes[0].outputs
            assert len(layerNodes) == 1, "Currently only support sequential networks (one node per layer), found {}".format( len(layerNodes) )
            current_layer: Layer = self.getLayer( layerNodes[0] )
            if current_layer is None: break
            keras_model.add( current_layer )
        masterNode["model"] = keras_model
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
        self.tensorboard = TensorBoard( log_dir=Archive.getLogDir(), histogram_freq=0, write_graph=True )
        self.stop_condition = "minValTrain"
        self.performanceTracker = PerformanceTracker( self.stop_condition )
        self.reseed()

    def getModel(self, node: OpNode ) -> Tuple[MasterNode,Model]:
        input_connection: WorkflowConnector = node.inputs[0]
        master_node = input_connection.connection
        assert isinstance( master_node, MasterNode ), "Training Kernel is not connected to a network!"
        return master_node, master_node.getParm("model")

    def buildLearningModel(self, node: MasterNode, model: Model ):
        optArgs = node.getParms( ["lr", "decay", "momentum", "nesterov" ] )
        sgd = SGD( **optArgs )
        model.compile(loss=node.getParm("loss","mse"), optimizer=sgd, metrics=['accuracy'])
        if self.weights is not None: model.set_weights(self.weights)

    def fitModel(self, master_node: MasterNode, train_node: OpNode, model: Model, inputDset: EDASDataset) -> EDASDataset:
        batchSize = master_node.getParm( "batchSize", 200 )
        nEpocs = master_node.getParm( "epochs", 600 )
        validation_fract = master_node.getParm( "valFraction", 0.2 )
        shuffle = master_node.getParm( "shuffle", True )
        inputData = self.getTrainingData( master_node, inputDset, 1 )
        targetData = self.getTargetData( train_node, inputDset, 1 )
        history: History = model.fit( inputData[0], targetData[0], batch_size=batchSize, epochs=nEpocs, validation_split=validation_fract, shuffle=shuffle, callbacks=[self.tensorboard,self.performanceTracker], verbose=0 )
        arrays = { id: self.getDataArray( history, id, nEpocs ) for id in [ "loss", "val_loss" ] }
        return EDASDataset( arrays, inputDset.attrs )

    def getDataArray(self, history: History, id: str, nEpochs: int, transforms = [] )-> EDASArray:
        data = xa.DataArray(history.history[id], coords=[ range( nEpochs ) ], dims=["epochs"])
        return EDASArray( id, None, data, transforms )

    def processInputCrossSection( self, request: TaskRequest, train_node: OpNode, inputDset: EDASDataset, products: List[str] ) -> EDASDataset:
        master_node, model = self.getModel( train_node )
        self.buildLearningModel( master_node, model )
        return self.fitModel( master_node, train_node, model, inputDset )

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


class PerformanceTracker(Callback):
    def __init__( self, _stopCond, **kwargs ):
        super(PerformanceTracker,self).__init__()
        self.logger = logging.getLogger()
        self.stopCond = _stopCond
        self.val_loss_history = None
        self.training_loss_history = None
        self.nIters = 0
        self.verbose = kwargs.get( "verbose", False )
        self.termIters = kwargs.get('earlyTermIndex', -1 )

    def on_train_begin(self, logs=None):
        self.minValLoss = sys.float_info.max
        self.minTrainLoss = sys.float_info.max
        self.nSteps = 0
        self.nEpoc = 0
        self.epochCount = 0
        self.best_weights = None
        self.nUphillIters = -1

    def on_train_end(self, logs=None):
        self.nIters += 1
        val_loss = np.array(self.model.history.history['val_loss'])
        training_loss = np.array(self.model.history.history['loss'])
        self.val_loss_history = self.intersect_add( self.val_loss_history, val_loss )
        self.training_loss_history = self.intersect_add(self.training_loss_history, training_loss)

    @staticmethod
    def intersect_add( data0, data1 ):
        # type: (np.ndarray,np.ndarray) -> np.ndarray
        if data1 is None: return data0
        if data0 is None: return data1
        len = min( data0.shape[0], data1.shape[0] )
        return data0[0:len] + data1[0:len]

    def on_epoch_end(self, epoch, logs=None):
        val_loss = self.model.history.history.get('val_loss',None)
        training_loss = self.model.history.history.get('loss',None)
        if self.verbose:
            tloss = training_loss[-1] if training_loss else "UNDEF"
            vloss = val_loss[-1] if val_loss else "UNDEF"
            self.logger.info( "* I[{0}]-E[{1}]-> training_loss: {2}, val_loss: {3}".format( self.nIters, self.epochCount, tloss, vloss ) )
        if val_loss and training_loss:
            vloss, tloss = val_loss[-1], training_loss[-1]
            self._processIter( vloss, tloss )
        if ( self.termIters > 0 ) and ( self.nUphillIters >= self.termIters ):
            self.logger.info( "Stopping training at iteration: " + str( self.nIters ) )
            self.model.stop_training = True
        self.epochCount = self.epochCount + 1

    def _processIter(self, valLoss, trainLoss ):
        self.nSteps += 1
        if (valLoss < self.minValLoss) :
            if (self.stopCond == "minVal") or ((self.stopCond == "minValTrain") and (trainLoss <= valLoss)):
                self.minValLoss = valLoss
                self.minTrainLoss = trainLoss
                self.nEpoc = self.nSteps
                self.nUphillIters = 0
                self.best_weights = copy.deepcopy(self.model.get_weights())
        elif self.nUphillIters >= 0:
            self.nUphillIters += 1

    def getHistory(self):
        return (self.training_loss_history / self.nIters, self.val_loss_history / self.nIters)

    def getWeights(self):
        return self.best_weights


