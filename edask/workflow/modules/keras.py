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
from edask.workflow.learning import FitResult, PerformanceTracker

def notNone( x ): return x is not None

class LayerKernel(OpKernel):
    def __init__( self ):
        Kernel.__init__( self, KernelSpec("layer", "Layer Kernel","Represents a layer in a neural network." ) )
        self.parent = "keras.network"

class KerasModel:

    @classmethod
    def getLayer( cls, layerNode: OpNode, inputDset: Optional[EDASDataset] = None, **kwargs ) -> Optional[Layer]:
        if layerNode.name != "keras.layer": return None
        args: Dict[str,Any] = { **layerNode.getMetadata( ignore=["input", "result", "axis", "axes", "name"] ), **kwargs }
        type = args.get("type","dense")
        if inputDset is not None:
            axes = layerNode.axes
            assert axes, "Must use 'axis' parameter in first layer to specify input coordinate"
            sizes = [ inputDset.getCoord(coord_name).size for coord_name in axes ]
            args["input_dim"] = reduce(mul, sizes, 1)
        if type == "dense":
            return Dense( **cls.parseArgs( args ) )
        else:
            raise Exception( "Unrecognized layer type: " + type )

    @classmethod
    def parseArgs(cls, args: Dict[str,Any] ) -> Dict[str,Any]: return { key: cls.parseValue( value ) for key,value in args.items() }

    @classmethod
    def parseValue(cls, value: Any ) -> Any:
        try: return int( value )
        except:
            try: return float( value )
            except:
                return value

    @classmethod
    def getLayers( cls, inputLayerNode: OpNode, inputDset: Optional[EDASDataset] = None, **kwargs ) -> Tuple[List[Layer],List[OpNode]] :
        keras_layers: List[Layer] = []
        input_layer: Layer = KerasModel.getLayer( inputLayerNode, inputDset )
        keras_layers.append( input_layer )
        orderedOpNodes = [ inputLayerNode ]
        layerNode = inputLayerNode
        while len(layerNode.outputs):
            current_layer: Layer = KerasModel.getLayer( layerNode.outputs[0] )
            if current_layer is None: break
            assert len(layerNode.outputs) == 1, "Currently only support sequential networks (one node per layer), found {}".format( len(layerNode.outputs) )
            keras_layers.append( current_layer )
            layerNode = layerNode.outputs[0]
            orderedOpNodes.append( layerNode )
        return ( keras_layers, orderedOpNodes )

    @classmethod
    def instantiateLayers( cls, layers: List[OpNode], inputDset: Optional[EDASDataset] = None, **kwargs ) -> List[Layer]:
        keras_layers: List[Layer] = [ KerasModel.getLayer( layers[0], inputDset ) ]
        for layer in layers[1:]: keras_layers.append( KerasModel.getLayer( layer ) )
        return keras_layers

    @classmethod
    def getModel( cls, keras_layers: List[Layer] ) -> Model:
        keras_model = Sequential()
        for keras_layer in keras_layers:
            keras_model.add( copy.deepcopy(keras_layer) )
        return keras_model

    @classmethod
    def unpackWeights( cls, id: str, wts: List[np.ndarray] )-> Dict[str,EDASArray]:
        arrays: Dict[str,EDASArray] = {}
        nLayers = int( len(wts)/2 )
        for iLayer in range(nLayers):
            wts_array = wts[2*iLayer]
            bias_array = wts[2*iLayer+1]
            inputDim =  ( 'n'+str(iLayer),   range(wts_array.shape[0]) )
            nodeDim  =  ( 'n'+str(iLayer+1), range(wts_array.shape[1]) )
            wtsId = id+"-wts"+str(iLayer)
            arrays[wtsId] = EDASArray( wtsId, None, xa.DataArray( wts_array, coords=( inputDim, nodeDim )  ), [] )
            biasId = id+"-bias"+str(iLayer)
            arrays[biasId] = EDASArray( biasId, None, xa.DataArray( bias_array, coords=( nodeDim, ) ), [] )
        return arrays

    @classmethod
    def packWeights( cls, id: str, modelData: EDASDataset )-> List[np.ndarray]:
        nLayers = modelData["nlayers"]
        weights: List[np.ndarray] = []
        for iLayer in range(nLayers):
            weights.append(modelData.getArray( id+"-wts"+str(iLayer) ).nd)
            weights.append(modelData.getArray(id + "-bias" + str(iLayer) ).nd)
        return weights

    @classmethod
    def map(cls, id: str, model: Model, variable: EDASArray) -> EDASArray:
        xarray = variable.xr
        result =  model.predict( xarray.values )
        coord =  xarray.coords[ xarray.dims[0] ]
        xresult = xa.DataArray( result, coords=( (xarray.dims[0],coord), ("nodes", range( result.shape[1] )) ) )
        return variable.updateXa( xresult, id )

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
        modelPath = Archive.getFilePath(node.getParm('proj'), node.getParm("exp"), "model" )
        modelData: EDASDataset = EDASDataset.open_dataset( modelPath )
        layersSpec = modelData["layers"]
        assert layersSpec, "Missing levels spec in model data"
        layerNodes = [ OpNode.deserialize(spec) for spec in layersSpec.split(";") ]
        layers = KerasModel.instantiateLayers( layerNodes )
        model = KerasModel.getModel( layers )
        weights = KerasModel.packWeights( "finalWts", modelData )
        model.set_weights( weights )
        input_size = weights[0].shape[0]
        input = self.getNetworkInput( node, variable, input_size )
        return [ KerasModel.map( "predict", model, input ) ]

    def getNetworkInput(self, node: OpNode, variable: EDASArray, input_size: int ):
        if len( node.axes ):
            return variable.transpose() if node.axes[0] == variable.dims[0] else variable
        else:
            candidates = [i for i, aval in enumerate(variable.xr.shape) if aval == input_size ]
            assert len(candidates) < 2, "Can't infer axis for input to model, must use 'axis' parameter"
            assert len(candidates) > 0, "Network input is of improper shape: " + str(variable.xr.shape)
            return variable.transpose() if candidates[0] == 0 else variable

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
        inputData = self.getTrainingData( master_node, inputDset, 1 )
        targetData = self.getTargetData( train_node, inputDset, 1 )
        initial_weights = model.get_weights()
        history: History = model.fit( inputData[0], targetData[0], batch_size=batchSize, epochs=nEpocs, validation_split=validation_fract, shuffle=shuffle, callbacks=[self.tensorboard,performanceTracker], verbose=0 )
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