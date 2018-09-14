import matplotlib.pyplot as plt
from edask.workflow.data import EDASDataset
import xarray as xa
import numpy as np

class ResultsPlotter:
    def plotPerformance( self, results: EDASDataset, title, **kwargs ):
        plt.title(title)
        valLoss: np.ndarray = results.getArray("val_loss").xr.values
        trainlLoss: np.ndarray = results.getArray("loss").xr.values
        x = range( valLoss.shape[0] )
        plt.plot( x, valLoss, "r-", label="Validation Loss" )
        plt.plot( x, trainlLoss, "b--", label="Training Loss" )
        plt.legend()
        plt.show()


plotter = ResultsPlotter()