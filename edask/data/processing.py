import os, sys, math, datetime
import numpy as np
import pandas as pd
import xarray as xr
from eofs.xarray import Eof

class Parser(object):

    @staticmethod
    def sparm( lines, name, value ):
        lines.append(  "@P:" + name + "=" + str(value) )

    @staticmethod
    def sparms( lines, parms ):
        for item in parms.items():
            Parser.sparm( lines, item[0], item[1] )

    @staticmethod
    def sdict( parms ):
        return ",".join( [ item[0] + ":" + str(item[1]) for item in parms.items() ] )

    @staticmethod
    def sarray( lines, name, data ):
        if data is not None:
            sdata = [ str(v) for v in data ]
            lines.append( "@A:" + name + "=" + ",".join(sdata) )

    @staticmethod
    def swts( lines, name, weights ):
        # type: (list[str], str, list[np.ndarray]) -> None
        wt_lines = []
        for wt_layer in weights:
            shape = ",".join( [ str(x) for x in wt_layer.shape ] )
            data = ",".join( [ str(x) for x in wt_layer.flat ] )
            wt_lines.append( "|".join( [shape,data] ) )
        lines.append( "@W:" + name + "=" + ";".join(wt_lines) )

    @staticmethod
    def rwts( spec ):
        # type: (str) -> list[np.ndarray]
        sarrays = spec.split(";")
        return [ np.loads(w) for w in sarrays ]

    @staticmethod
    def raint( spec ):
        # type: (str) -> list[int]
        if spec is None: return None
        elif isinstance( spec, str ):
            spec = spec.strip().strip("[]")
            return [ int(x) for x in spec.split(",")]
        else: return spec

    @staticmethod
    def rdict( spec ):
        # type: (str) -> dict
        rv = {}
        for item in spec.split(","):
            if item.find(":") >= 0:
                toks = item.split(":")
                rv[toks[0]] = toks[1].strip()
        return rv

    @staticmethod
    def ro( spec ):
        # type: (str) -> object
        if spec is None: return None
        elif isinstance( spec, str ) and ( spec.lower() == "none" ): return None
        else: return spec


class Analytics(object):
    smoothing_kernel = np.array([.13, .23, .28, .23, .13])
    months = "jfmamjjasonjfmamjjason"

    @staticmethod
    def normalize( data, axis=0 ):
        # type: (np.ndarray) -> np.ndarray
        std = np.std( data, axis )
        return data / std

    @staticmethod
    def intersect_add( data0, data1 ):
        # type: (np.ndarray,np.ndarray) -> np.ndarray
        if data1 is None: return data0
        if data0 is None: return data1
        len = min( data0.shape[0], data1.shape[0] )
        return data0[0:len] + data1[0:len]

    @staticmethod
    def center( data, axis=0 ):
        # type: (np.ndarray) -> np.ndarray
        mean = np.average( data, axis )
        return data - mean

    @staticmethod
    def getMonthFilterIndices(filter):
        try:
            start_index = int(filter)
            return (start_index, 1)
        except:
            try:
                start_index = "xjfmamjjasondjfmamjjasond".index(filter.lower())
                return ( start_index, len(filter) )
            except ValueError:
                raise Exception( "Unrecognizable filter value: '{0}' ".format(filter) )

    @classmethod
    def lowpass( cls, data ):
        # type: (np.ndarray) -> np.ndarray
        return np.convolve( data, cls.smoothing_kernel, "same")

    @classmethod
    def decycle( cls, dates, data ):
        # type: (list[datetime.date],np.ndarray) -> np.ndarray
        if len(data.shape) == 1: data = data.reshape( [ data.shape[0], 1 ] )
        times = pd.DatetimeIndex( data=dates, name="time" )                                             # type: pd.DatetimeIndex
        ds = xr.Dataset({'data': ( ('time', 'series'), data) },   {'time': times } )                    # type: xr.Dataset
        climatology = ds.groupby('time.month').mean('time')                                             # type: xr.Dataset
        anomalies = ds.groupby('time.month') - climatology                                              # type: xr.Dataset
        return anomalies["data"].data

    @classmethod
    def orthoModes( cls, data, nModes ):
        # type: (np.ndarray, int) -> np.ndarray
        eof = Eof( data, None, False, False )
        result = eof.eofs( 0, nModes )  # type: np.ndarray
        result = result / ( np.std( result ) * math.sqrt(data.shape[1]) )
        return result.transpose()

