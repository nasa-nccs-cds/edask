
import logging, os, time
import cdms2, datetime, matplotlib, math
from mpl_toolkits..basemap import Basemap
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from .pcProject import *
import numpy as np
from typing import List, Any

MPL = 0
VCS = 1

class ResultsPlotter:

    def __init__(self, projectDir ):
        self.dir = projectDir
        self.plotter = PlotMgr()

    def plotPCs( self, project, experiment, nCols=4 ):
        self.plotter.mpl_timeplot( project, experiment, nCols )

    def plotEOFs( self, project, experiment, nCols=4, plotPkg=VCS ):
        if( plotPkg == VCS ): self.plotter.vcs_plot_eofs( project, experiment, nCols )
        else: self.plotter.mpl_spaceplot( project.outfilePath( experiment, EOF ), nCols, 0 , True )


class PlotMgr:

    def __init__(self):
        self.logger = logging.getLogger('cwt.wps')


    def graph_data(self , data, title="" ):
        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        xvalues = range( len(data ) )
        ax.set_title( title )
        ax.plot( xvalues, data )
        plt.show()

    def mpl_timeplot( self, project: Project, experiment, numCols = 4 ):
        dataPath = project.outfilePath( experiment, PC )
        if dataPath:
            for k in range(0,30):
                if( os.path.isfile(dataPath) ):
                    self.logger.info( "Plotting file: " +  dataPath )
                    variables = project.getVariables( experiment, PC )
                    self.mpl_timeplot_variables( variables, numCols )
                    return
                else: time.sleep(1)

    def mpl_timeplot_variables( self, variables: List[cdms2.tvariable.TransientVariable], numCols: int = 4 ) -> plt.Figure:
        fig: plt.Figure = plt.figure()
        iplot = 1
        nCols = min( len(variables), numCols )
        nRows = math.ceil( len(variables) / float(nCols) )
        ax = None
        for timeSeries in variables:
            varName = timeSeries.id
            self.logger.info( "  ->  Plotting variable: " +  varName + ", subplot: " + str(iplot) )
            long_name = timeSeries.attributes.get('long_name')
            datetimes = [datetime.datetime(x.year, x.month, x.day, x.hour, x.minute, int(x.second)) for x in timeSeries.getTime().asComponentTime()]
            dates = matplotlib.dates.date2num(datetimes)
            ax = fig.add_subplot( nRows, nCols, iplot )
            title = varName if long_name is None else long_name
            ax.set_title( title )
            ax.plot(dates, timeSeries.data )
            ax.xaxis.set_major_formatter( mdates.DateFormatter('%b %Y') )
            ax.grid(True)
            iplot = iplot + 1
        fig.autofmt_xdate()
        plt.show()
        return fig

    def mpl_comparison_timeplot_variables(self, ref_var: cdms2.tvariable.TransientVariable, variables: list[cdms2.tvariable.TransientVariable], nModes: int) -> plt.Figure:
        fig: plt.Figure = plt.figure()
        varNames = "-".join( [ timeSeries.id for timeSeries in variables ] )
        self.logger.info("  ->  Plotting variable: " + varNames )
        long_names = "-".join( [ timeSeries.attributes.get('long_name')for timeSeries in variables ] )
        datetimes = [datetime.datetime(x.year, x.month, x.day, x.hour, x.minute, int(x.second)) for x in ref_var.getTime().asComponentTime()]
        dates =  matplotlib.dates.date2num(datetimes)
        for iMode in range(nModes):
            ax = fig.add_subplot(2, int( math.ceil( nModes/2.0 ) ), iMode+1 )
            title = "Mode " + str(iMode) + ": " + ( varNames if not long_names else long_names )
            ax.set_title(title)
            modeVars = [ var[iMode] for var in variables ]
            fmts = [ "b-", "r--", "g-.", "y:",]
            for iM in range(len(modeVars)):
                timeSeries = modeVars[iM]
                ax.plot( dates, self.norm( timeSeries.data.squeeze() ), fmts[iM] )
                ax.xaxis.set_major_formatter( mdates.DateFormatter('%b %Y') )
                ax.grid(True)
        fig.autofmt_xdate()
        plt.show()
        return fig

    def norm(self, data: np.ndarray, axis=0 ) -> np.ndarray:
        mean = data.mean(axis)
        centered = (data - mean)
        std = centered.std(axis)
        return centered / std

    def getAxis(self, axes, atype ):
        for axis in axes:
            try:
                if( (atype == "X") and self.isLongitude(axis) ): return axis[:]
                if( (atype == "Y") and self.isLatitude(axis) ): return axis[:]
                if( (atype == "Z") and axis.isLevel() ): return axis[:]
                if( (atype == "T") and axis.isTime() ): return axis[:]
            except Exception as ex:
                self.logger.error(  "Exception in getAxis({0})".format(atype) )
        return None

    def isLongitude(self, axis ):
        id = axis.id.lower()
        hasAxis = hasattr(axis, 'axis')
        isX = axis.axis == 'X'
        if ( hasAxis and isX ): return True
        return ( id.startswith( 'lon' ) )

    def isLatitude(self, axis ):
        id = axis.id.lower()
        if (hasattr(axis, 'axis') and axis.axis == 'Y'): return True
        return ( id.startswith( 'lat' ) )

    def getRowsCols( self, number ):
        largest_divisor = 1
        for i in range(2, number):
            if( number % i == 0 ):
                largest_divisor = i
        complement = number/largest_divisor
        return (complement,largest_divisor) if( largest_divisor > complement ) else (largest_divisor,complement)

    def mpl_plot(self, dataPath, nCols=2 ):
        f = cdms2.openDataset(dataPath)
        var = f.variables.values()[0]
        naxes = self.getNAxes( var.shape )
        self.mpl_timeplot( dataPath )

    def getNAxes(self, shape ):
        naxes = 0
        for axisLen in shape:
            if( axisLen > 1 ):
                naxes = naxes + 1
        return naxes

    def mpl_spaceplot( self, dataPath, numCols=4, timeIndex=0, smooth=False ):
        if dataPath:
            for k in range(0,30):
                if( os.path.isfile(dataPath) ):
                    self.logger.info( "Plotting file: " +  dataPath )
                    f = cdms2.openDataset(dataPath) # type: cdms2.dataset.CdmsFile
                    vars = f.variables.values()
                    axes = f.axes.values()
                    lons = self.getAxis( axes , "X" )
                    lats = self.getAxis( axes , "Y" )
                    fig = plt.figure()
                    varNames = list( map( lambda v: v.id, vars ) )
                    varNames.sort()
                    nCols = min( len(varNames), numCols )
                    nRows = math.ceil( len(varNames) / float(nCols) )
                    iplot = 1
                    for varName in varNames:
                        if not varName.endswith("_bnds"):
                            try:
                                variable = f( varName )
                                if len( variable.shape ) > 1:
                                    m = Basemap( llcrnrlon=lons[0],
                                                 llcrnrlat=lats[0],
                                                 urcrnrlon=lons[len(lons)-1],
                                                 urcrnrlat=lats[len(lats)-1],
                                                 epsg='4326',
                                                 lat_0 = lats.mean(),
                                                 lon_0 = lons.mean())
                                    ax = fig.add_subplot( nRows, nCols, iplot )
                                    ax.set_title(varName)
                                    lon, lat = np.meshgrid( lons, lats )
                                    xi, yi = m(lon, lat)
                                    smoothing = 'gouraud' if smooth else 'flat'
                                    spatialData = variable( time=slice(timeIndex,timeIndex+1), squeeze=1 )
                                    cs2 = m.pcolormesh(xi, yi, spatialData, cmap='jet', shading=smoothing )
                                    lats_space = abs(lats[0])+abs(lats[len(lats)-1])
                                    m.drawparallels(np.arange(lats[0],lats[len(lats)-1], round(lats_space/5, 0)), labels=[1,0,0,0], dashes=[6,900])
                                    lons_space = abs(lons[0])+abs(lons[len(lons)-1])
                                    m.drawmeridians(np.arange(lons[0],lons[len(lons)-1], round(lons_space/5, 0)), labels=[0,0,0,1], dashes=[6,900])
                                    m.drawcoastlines()
                                    m.drawstates()
                                    m.drawcountries()
                                    cbar = m.colorbar(cs2,location='bottom',pad="10%")
                                    self.logger.debug(  "Plotting variable: " + varName )
                                    iplot = iplot + 1
                            except:
                                self.logger.debug( "Skipping variable: " + varName )

                    fig.subplots_adjust(wspace=0.1, hspace=0.1, top=0.95, bottom=0.05)
                    plt.show()
                    return
                else: time.sleep(1)

    def print_Mdata(self, dataPath ):
        for k in range(0,30):
            if( os.path.isfile(dataPath) ):
                f = cdms2.openDataset(dataPath)
                for variable in f.variables.values():
                    self.logger.info( "Produced result " + variable.id + ", shape: " +  str( variable.shape ) + ", dims: " + variable.getOrder() + " from file: " + dataPath )
                    self.logger.info( "Data Sample: " + str( variable[0] ) )
                    return
            else: time.sleep(1)



    def print_data(self, dataPath ):
        for k in range(0,30):
            if( os.path.isfile(dataPath) ):
                try:
                    f = cdms2.openDataset(dataPath) # """:type : cdms2.CdmsFile """
                    varName = f.variables.values()[0].id
                    spatialData = f( varName ) # """:type : cdms2.FileVariable """
                    self.logger.info( "Produced result, shape: " +  str( spatialData.shape ) + ", dims: " + spatialData.getOrder() )
        #            self.logger.info( "Data: \n" + ', '.join( str(x) for x in spatialData.getValue() ) )
                    self.logger.info( "Data: \n" + str( spatialData.squeeze().flatten().getValue() ) )
                except Exception:
                    self.logger.error( " ** Error printing result data ***")
                return
            else: time.sleep(1)

