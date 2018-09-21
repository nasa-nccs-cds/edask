import matplotlib.pyplot as plt
from edask.workflow.data import EDASDataset
import xarray as xa
import numpy as np

class ResultsPlotter:
    def plotPerformance( self, results: EDASDataset, title, **kwargs ):
        plt.title(title)
        print( "Plotting: " + ",".join( list(results.ids) ) )
        valLoss: np.ndarray = results.getArray("val_loss").xr.values
        trainlLoss: np.ndarray = results.getArray("loss").xr.values
        x = range( valLoss.shape[0] )
        plt.plot( x, valLoss, "r-", label="Validation Loss" )
        plt.plot( x, trainlLoss, "b--", label="Training Loss" )
        plt.legend()
        plt.show()

    def plotPerformanceXa( self, results: xa.Dataset, title, **kwargs ):
        plt.title(title)
        print("XA-Plotting: " + title + ", keys: " + str(list(results.data_vars.keys())) )
        valLoss: np.ndarray = results.data_vars["val_loss"].values
        trainlLoss: np.ndarray = results.data_vars["loss"].values
        x = range( valLoss.shape[0] )
        plt.plot( x, valLoss, "r-", label="Validation Loss" )
        plt.plot( x, trainlLoss, "b--", label="Training Loss" )
        plt.legend()
        plt.show()

    def plotPrediction( self, results: EDASDataset, title, **kwargs ):
        plt.title(title)
        print( "Plotting: " + ",".join( list(results.ids) ) )
        prediction: np.ndarray = results.getArray("prediction").xr.values
        target: np.ndarray = results.getArray("target").xr.values
        x = range( prediction.shape[0] )
        plt.plot( x, prediction, "r-", label="prediction" )
        plt.plot( x, target, "b--", label="target" )
        plt.legend()
        plt.show()


plotter = ResultsPlotter()