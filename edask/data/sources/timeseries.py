import os, datetime, re
import xarray as xa
import numpy as np
import pandas as pd
from collections import OrderedDict
from datetime import datetime, timezone
from typing import  List, Dict, Any, Sequence, Union, Optional, ValuesView, Tuple

import abc

class TimeConversions:

    @staticmethod
    def toDatetime( dt64: np.datetime64 ) -> datetime:
        return datetime.fromtimestamp(dt64.astype(int) * 1e-9, tz=timezone.utc)

    @staticmethod
    def parseDate( sdate: str ) -> datetime:
        from dateutil.parser import parse
        if not sdate[-1].isalpha():  sdate = sdate + "Z"
        return  parse( sdate )

class TimeseriesData(object):

    def __init__(self, _dates: List[datetime.date], _series: List[Tuple[str,np.ndarray]] ):
        self.dates = _dates
        self.series = OrderedDict()
        for (k, v) in _series: self.series[k] = v
        self.output_size = len( self.series.keys() )

    @property
    def data(self) -> np.ndarray:
        return np.column_stack( self.series.values() )

    @property
    def xr(self) -> xa.DataArray:
        coords = [ [pd.to_datetime(d) for d in self.dates], list(self.series.keys()) ]
        array = xa.DataArray(self.data, coords=coords, dims=['t','m'])
        return array

    @property
    def ds(self) -> xa.Dataset:
        coords = [ [pd.to_datetime(d) for d in self.dates] ]
        arrays = [ xa.DataArray(series, name=name, coords=coords, dims=['t']) for name, series in self.series.items() ]
        return xa.Dataset( arrays, coords=coords )

    # def archive( self, project: str, experiment: str, merge_series=True ):
    #     path = Archive.getFilePath(project, experiment, "timeseries" )
    #     if merge_series:    self.xr.to_netcdf( path=path, mode='w' )
    #     else:               self.ds.to_netcdf( path=path, mode='w' )

class DataSource(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name: str ):
        self.name = name
        thisFile = os.path.realpath(__file__)
        self.rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(thisFile))))

    def getDataFilePath(self, type: str, ext: str ) -> str:
        return os.path.join(self.rootDir, "resources", "data", type, self.name + "." + ext )

    def getTimeseries( self, timeRange, **kwargs ): raise NotImplementedError

    @abc.abstractmethod
    def serialize(self,lines):
        return None

class CDuration(object):

    MONTH = "M"
    YEAR = "Y"

    def __init__(self, _length, _unit):
        self.length = _length
        self.unit = _unit

    @classmethod
    def months(cls, length):
        return CDuration( length, cls.MONTH )

    @classmethod
    def years(cls, length):
        return CDuration( length, cls.YEAR )

    def inc( self, increment: int )-> "CDuration":
        return CDuration(self.length + increment, self.unit)

    def __add__(self, other: "CDuration" )-> "CDuration":
        assert self.unit == other.unit, "Incommensurable units in CDuration add operation"
        return CDuration( self.length + other.length , self.unit )


    def __sub__(self, other: "CDuration" )-> "CDuration":
        assert self.unit == other.unit, "Incommensurable units in CDuration sub operation"
        return CDuration(self.length - other.length, self.unit )


class CDate(object):

    def __init__(self, Year, Month, Day):
        # type: (int,int,int) -> None
        self.year  = Year
        self.month = Month
        self.day   = Day

    @classmethod
    def new(cls, date_str: str)-> "CDate":
        '''Call as: d = Date.from_str('2013-12-30')
        '''
        year, month, day = map(int, date_str.split('-'))
        return cls(year, month, day)

    def __str__(self) -> str:
        return "-".join( map(str, [self.year,self.month,self.day] ) )

    def inc(self, duration: CDuration ) -> "CDate":
        if duration.unit == CDuration.YEAR:
            return CDate( self.year + duration.length, self.month, self.day )
        elif duration.unit == CDuration.MONTH:
            month_inc = ( self.month + duration.length - 1 )
            new_month = ( month_inc % 12 ) + 1
            new_year = self.year + month_inc / 12
            return CDate( new_year, new_month, self.day )
        else: raise Exception( "Illegal unit value: " + str(duration.unit) )

class CTimeRange(object):

    def __init__(self, start: CDate, end: CDate ):
        self.startDate = start
        self.endDate = end
        self.dateRange = self.getDateRange()

    def serialize(self):
        return str(self.startDate) + "," + str(self.endDate)

    @staticmethod
    def deserialize( spec ):
        if spec is None:
            return None
        elif isinstance(spec, CTimeRange):
            return spec
        elif isinstance( spec, str ):
            dates = spec.split(",")
            return CTimeRange( CDate.new(dates[0]), CDate.new(dates[1]) )
        else:
            raise Exception( "Object of type {0} cannot be converted to a CTimeRange".format( spec.__class__.__name__ ) )


    @classmethod
    def new(cls, start: str, end: str ) -> "CTimeRange":
        return CTimeRange( CDate.new(start), CDate.new(end) )

    def shift(self, duration: CDuration ) -> "CTimeRange":
        return CTimeRange(self.startDate.inc(duration), self.endDate.inc(duration) )

    def extend(self, duration: CDuration ) -> "CTimeRange":
        return CTimeRange(self.startDate, self.endDate.inc(duration) )

#    def selector(self)-> Selector:
#        return Selector( time=(str(self.startDate), str(self.endDate)) )

    def getDateRange(self)-> List[ datetime.date ]:
        return  [ self.toDate( dateStr ) for dateStr in [ str(self.startDate), str(self.endDate) ] ]

    def toDate( self, dateStr: str ) -> datetime.date:
        toks = [ int(tok) for tok in dateStr.split("-") ]
        return datetime.date( toks[0], toks[1], toks[2] )

    def inDateRange( self, date: datetime.date ) -> bool:
        return date >= self.dateRange[0] and date <= self.dateRange[1]

class TimeIndexer(object):

    year = "xjfmamjjasondjfmamjjasond"
    months = [ "x", "jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]

    @classmethod
    def getMonthIndices( cls, selection:str ) -> List[int]:
        sel = selection.lower()
        try:
            return [ int(sel) ]
        except:
            try:
                return [ cls.months.index(sel) ]
            except ValueError:
                try:
                    start_index = cls.year.index(sel)
                    return list( range( start_index, start_index + len(sel) ) )
                except ValueError:
                    raise Exception( "Unrecognizable filter value: '{0}' ".format(filter) )
