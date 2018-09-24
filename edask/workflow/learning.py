from edask.data.processing import Analytics, Parser
from typing import List, Dict, Sequence, Set, Iterable
import numpy as np
import sys, copy, logging
from .kernel import Kernel, KernelSpec, EDASDataset, OpKernel
import xarray as xa
from edask.process.operation import WorkflowNode, OpNode, MasterNode
from edask.workflow.data import EDASArray
from typing import List, Optional, Tuple, Dict, Any
from operator import mul
from functools import reduce
import copy, sys, logging, random, numpy as np
from keras.models import Sequential, Model
from keras.layers import Dense, Activation
from keras.engine.base_layer import Layer
from keras.callbacks import History, Callback

def notNone( x ): return x is not None

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
    def intersect_add( data0: np.ndarray, data1: np.ndarray )-> np.ndarray:
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


class FitResult(object):

    @staticmethod
    def getCombinedAverages( results: List["FitResult"] ) -> ( np.ndarray, np.ndarray, int ):
        ave_train_loss_sum = None
        ave_val_loss_sum = None
        nInstances = 0
        for result in results:
            ( ave_loss_history, ave_val_loss_history, nI ) = result.getAverages()
            ave_train_loss_sum = Analytics.intersect_add( ave_train_loss_sum, ave_loss_history * nI )
            ave_val_loss_sum =  Analytics.intersect_add(  ave_val_loss_sum,   ave_val_loss_history * nI )
            nInstances += nI
        return (None,None,nInstances) if ave_train_loss_sum is None else ( ave_train_loss_sum / nInstances, ave_val_loss_sum / nInstances, nInstances )

    @staticmethod
    def new( history: History, initial_weights: List[np.ndarray], final_weights: List[np.ndarray], training_loss: float, val_loss: float,  nEpocs: int )-> "FitResult":
        return FitResult( history.history['val_loss'], history.history['loss'], initial_weights, final_weights,  training_loss, val_loss, nEpocs )

    def __init__( self, _val_loss_history: Iterable[float], _train_loss_history: Iterable[float], _initial_weights: List[np.ndarray], _final_weights: List[np.ndarray], _training_loss: float,  _val_loss: float, _nEpocs: int ):
        self.logger = logging.getLogger()
        self.val_loss_history = np.array( _val_loss_history )
        self.train_loss_history = np.array( _train_loss_history )
        self.initial_weights = _initial_weights
        self.final_weights = _final_weights
        self.val_loss = float( _val_loss )
        self.train_loss = float( _training_loss )
        self.nEpocs = int( _nEpocs )
        self.ave_train_loss_history = None
        self.ave_val_loss_history = None
        self.nInstances = -1

    def getAverages(self):
        return (self.ave_train_loss_history, self.ave_val_loss_history, self.nInstances)

    def recordPerformance( self, performanceTracker: PerformanceTracker ):
        (self.ave_train_loss_history, self.ave_val_loss_history) = performanceTracker.getHistory()
        self.nInstances = performanceTracker.nIters
        return self

    def setPerformance( self, results: List["FitResult"] )-> "FitResult":
        (self.ave_train_loss_history, self.ave_val_loss_history, self.nInstances) = FitResult.getCombinedAverages(results)
        print( "Setting performance averages from {0} instances".format(self.nInstances) )
        return self

    def getScore( self, score_val_weight: float ):
        return score_val_weight * self.val_loss + self.train_loss

    def serialize( self, lines: List[str] ):
        lines.append( "#Result" )
        Parser.sparm( lines, "val_loss", self.val_loss )
        Parser.sparm( lines, "train_loss", self.train_loss )
        Parser.sparm( lines, "nepocs", self.nEpocs )
        Parser.sparm(lines, "ninstances", self.nInstances)
        Parser.sarray( lines, "val_loss_history", self.val_loss_history )
        Parser.sarray( lines, "train_loss_history", self.train_loss_history )
        Parser.sarray(lines, "ave_train_loss_history", self.ave_train_loss_history)
        Parser.sarray( lines, "ave_val_loss_history", self.ave_val_loss_history )
        Parser.swts( lines, "initial", self.initial_weights )
        Parser.swts( lines, "final", self.final_weights )

    @staticmethod
    def deserialize( lines: Iterable[str] ):
        active = False
        parms = {}
        arrays = {}
        weights = {}
        for line in lines:
            if line.startswith("#Result"):
                active = True
            elif active:
                if line.startswith("#"): break
                elif line.startswith("@P:"):
                    toks = line.split(":")[1].split("=")
                    parms[toks[0]] = toks[1].strip()
                elif line.startswith("@A:"):
                    toks = line.split(":")[1].split("=")
                    arrays[toks[0]] = [ float(x) for x in toks[1].split(",") ]
                elif line.startswith("@W:"):
                    toks = line.split(":")[1].split("=")
                    wts = []
                    for wtLine in toks[1].split(";"):
                        wtToks = wtLine.split("|")
                        shape = [int(x) for x in wtToks[0].split(",")]
                        data = [float(x) for x in wtToks[1].split(",")]
                        wts.append( np.array(data).reshape(shape) )
                    weights[toks[0]] = wts
        rv = FitResult( arrays["val_loss_history"], arrays["train_loss_history"], weights["initial"], weights["final"], parms["train_loss"],  parms["val_loss"], parms["nepocs"] )
        rv.ave_val_loss_history = arrays.get("ave_val_loss_history",None)
        if rv.ave_val_loss_history is not None:
            rv.ave_val_loss_history = np.array( rv.ave_val_loss_history )
        rv.ave_train_loss_history = arrays.get("ave_train_loss_history", None)
        if rv.ave_train_loss_history is not None:
            rv.ave_train_loss_history = np.array( rv.ave_val_loss_history )
        rv.nInstances = parms.get("ninstances",-1)
        return rv


    def valLossHistory(self):
        return self.val_loss_history

    def lossHistory(self):
        return self.train_loss_history

    def isMature(self):
        return self.val_loss < self.train_loss

    def __lt__(self, other: "FitResult" )-> bool:
        return self.val_loss < other.val_loss

    def __gt__(self, other: "FitResult" )-> bool:
        return self.val_loss > other.val_loss

    def __eq__(self, other: "FitResult" )-> bool:
        return self.val_loss == other.val_loss

    @staticmethod
    def getBest( results: List["FitResult"] )-> "FitResult":
        bestResult = None  # type: FitResult
        for result in results:
            if isinstance(result, str):
                raise Exception( "A worker raised an Exception: " + result )
            elif result and result.isMature:
                if bestResult is None or result < bestResult:
                    bestResult = result
        return bestResult.setPerformance( results )


class KerasModel:

    @classmethod
    def getLayer( cls, layerNode: OpNode, layer_index: int, inputDset: Optional[EDASDataset] = None, **kwargs ) -> Optional[Layer]:
        if layerNode.name != "keras.layer": return None
        args: Dict[str,Any] = { **layerNode.getMetadata( ignore=["input", "result", "axis", "axes", "name", "inputSize"] ), **kwargs }
        type = args.get("type","dense")
        inputDim = int(layerNode["inputSize"])
        units = int(layerNode["units"])
        if layer_index == 0:
            if inputDset is not None:
                axes = layerNode.axes
                assert axes, "Must use 'axis' parameter in first layer to specify input coordinate"
                sizes = [ inputDset.getCoord(coord_name).size for coord_name in axes ]
                args["input_dim"] = reduce(mul, sizes, 1)
            else: args["input_dim"] = inputDim

        if type == "dense":
            kwargs = cls.parseArgs( args )
            layer =  Dense( **kwargs )
#            layer.build( (inputDim,units) )
            return layer
        else:
            raise Exception( "Unrecognized layer type: " + type )

    @classmethod
    def parseArgs(cls, args: Dict[str,Any] ) -> Dict[str,Any]: return { key: cls.parseValue( value ) for key,value in args.items() }

    @classmethod
    def parseValue(cls, value: Any ) -> Any:
        if isinstance(value,str) and value[0] == "[": return [ cls.parseValue(x) for x in value.strip("[]").split(",") ]
        try: return int( value )
        except:
            try: return float( value )
            except:
                return value

    @classmethod
    def getLayers( cls, inputLayerNode: OpNode, inputDset: EDASDataset, **kwargs ) -> Tuple[List[Layer],List[OpNode]] :
        keras_layers: List[Layer] = []
        layer_index = 0
        input_layer: Layer = KerasModel.getLayer( inputLayerNode, layer_index, inputDset )
        inputLayerNode["inputSize"] =inputDset.inputs[0].xr.shape[0]
        keras_layers.append( input_layer )
        orderedOpNodes = [ inputLayerNode ]
        layerNode = inputLayerNode
        while len(layerNode.outputs):
            layer_index = layer_index + 1
            current_layer: Layer = KerasModel.getLayer( layerNode.outputs[0], layer_index )
            if current_layer is None: break
            assert len(layerNode.outputs) == 1, "Currently only support sequential networks (one node per layer), found {}".format( len(layerNode.outputs) )
            input_shape = layerNode["units"]
            keras_layers.append( current_layer )
            layerNode = layerNode.outputs[0]
            layerNode["inputSize"] = input_shape
            orderedOpNodes.append( layerNode )
        return ( keras_layers, orderedOpNodes )

    @classmethod
    def instantiateLayers( cls, layers: List[OpNode], **kwargs ) -> List[Layer]:
        keras_layers: List[Layer] = [ KerasModel.getLayer( layers[0], 0 ) ]
        for idx,layer in enumerate(layers[1:]): keras_layers.append( KerasModel.getLayer( layer, idx+1 ) )
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
        print( "PREDICT WEIGHTS: " + str(model.get_weights()))
#        print( "PREDICT INPUT: " + str( list( xarray.values.flat ) ) )
#        print( "PREDICT MODEL: " + cls.describeModel(model) )
#        print( "PREDICT RESULT: " + str( list( result.flat ) ) )
        coord =  xarray.coords[ xarray.dims[0] ]
        xresult = xa.DataArray( result, coords=( (xarray.dims[0],coord), ("nodes", range( result.shape[1] )) ) )
        return variable.updateXa( xresult, id )

    @classmethod
    def describeModel(cls, model: Model ) -> str:
        return "Model {}:\n\t{}".format( model.name, "\n\t".join( [ "Layer {}: {}".format( layer.name, str(layer.get_config()) ) for layer in model.layers ] ) )

    @classmethod
    def getNetworkInput( cls, node: OpNode, variable: EDASArray, input_size: int ):
        if len( node.axes ):
            return variable.transpose() if node.axes[0] == variable.dims[0] else variable
        else:
            candidates = [i for i, aval in enumerate(variable.xr.shape) if aval == input_size ]
            assert len(candidates) < 2, "Can't infer axis for input to model, must use 'axis' parameter"
            assert len(candidates) > 0, "Network input is of improper shape: " + str(variable.xr.shape)
            return variable.transpose() if candidates[0] == 0 else variable

    @classmethod
    def getTrainingData(cls, model_node: WorkflowNode, inputDset: EDASDataset, required_size = None ) -> List[EDASArray]:
        train_input_ids = [ inp.name for inp in model_node.inputs ]
        assert (required_size == None) or (len( train_input_ids ) == required_size), "Train Kernel expects exactly {} input(s): got {}".format( required_size, len( train_input_ids ) )
        return cls.getInputData( train_input_ids, inputDset, model_node.axes[0], 1 )

    @classmethod
    def getTargetData(cls, train_node: WorkflowNode, inputDset: EDASDataset, required_size = None ) -> List[EDASArray]:
        target_input_ids = train_node.getParm("target","").split(",")
        assert (required_size == None) or (len( target_input_ids ) == required_size), "Train Kernel expects exactly {} target(s): got {}".format( required_size, len( target_input_ids ) )
        return cls.getInputData( target_input_ids, inputDset, train_node.axes[0], 0 )

    @classmethod
    def getInputData( cls, ids: List[str], inputDset: EDASDataset, dim: str, expectedDimIndex: int ) -> List[EDASArray]:
        train_inputs = list( filter( notNone, [ inputDset.getArray(id) for id in ids ] ) )
        assert len( train_inputs ), "Can't find input data for training, looking for {}, found {}".format( ids, inputDset.ids )
        return cls.getAlignedArrays( train_inputs, dim, expectedDimIndex )

    @classmethod
    def getAlignedArrays( cls, inputs: List[EDASArray], dim: str, expectedDimIndex: int ) -> List[EDASArray]:
        results: List[EDASArray] = []
        for array in inputs:
            try:
                dimIndex = array.dims.index( dim )
                array = array.T if dimIndex != expectedDimIndex else array
                results.append( array )
            except ValueError: raise Exception( "Can't find dim {} in network input dimensions: {}".format(dim,array.dims) )
        return results

