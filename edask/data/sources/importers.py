import os, datetime
import numpy as np
from typing import  List, Dict, Any, Sequence, Union, Optional, ValuesView, Tuple
import logging
from edask.data.sources.timeseries import DataSource, CTimeRange, TimeseriesData
import xarray as xa
from edask.collections.agg import Archive

class IITMDataSource(DataSource):

    # Types:   MONTHLY ANNUAL JF MAM JJAS OND
    # Names:   AI EPI NCI NEI NMI SPI WPI

    def __init__(self, name: str, _type: str):
        DataSource.__init__(self, name)
        self.type = _type.lower()
        self.dataFile = self.getDataFilePath("IITM","txt")
        self.logger = logging.getLogger()

    def serialize(self,lines):
        lines.append( "@IITMDataSource:" + ",".join( [ self.name, self.type ] ) )

    @staticmethod
    def deserialize( spec: str ) -> "IITMDataSource":
        toks =  spec.split(":")[1].split(",")
        return IITMDataSource( toks[0], toks[1].strip() )

    def getTypeIndices(self) -> Tuple[int,int]:
        if self.type.startswith("ann"):
            return 12, 6
        elif self.type == "jf":
            return 13, 2
        elif self.type == "mam":
            return 14, 4
        elif self.type == "jjas" or self.type == "monsoon":
            return 15, 8
        elif self.type == "ond":
            return 16, 11
        else:
            raise Exception( "Unrecognized Data source type: '{0}'".format(self.type) )

    def freq(self) -> str:
        if self.type == "monthly": return "M"
        else: return "Y"

    def splitLine(self, line ) -> ( str, List[str] ):
        parts = [ ]
        year = line[0:4]
        for iCol in range(17):
            start = 4 + iCol*7
            sval = line[start:start+7]
            if not sval: sval = "nan"
            parts.append( sval )
        return year, parts


    def getTimeseries(self, timeRange: Optional[CTimeRange]=None, **kwargs) -> TimeseriesData:
        dset = open(self.dataFile,"r")
        lines = dset.readlines()

        timeseries = []
        dates = []
        logging.info("TrainingData: name = {0}, time range = {1}".format(str(self.name), str(timeRange)))
        for iLine in range( len(lines) ):
            line = lines[iLine]
            year, line_elems = self.splitLine(line)
            if self.isYear(year):
                iYear = int(year)
                if self.type == "monthly":
                    for iMonth in range(0,12):
                        sval = line_elems[ iMonth ]
                        value = float( sval )
                        date = datetime.date(iYear, iMonth, 15)
                        if  not timeRange or timeRange.inDateRange(date):
                            timeseries.append( value )
                            dates.append( date )
                else:
                    colIndex, iMonth = self.getTypeIndices()
                    date = datetime.date(iYear, iMonth, 15)
                    if not timeRange or timeRange.inDateRange(date):
                        value = float( line_elems[ colIndex ] )
                        timeseries.append( value )
                        dates.append( date )
        tsarray = np.array( timeseries )

        return TimeseriesData( dates, [ ( self.type, tsarray ) ] )

    def isYear( self, s: str )-> bool:
        try:
            test_val = int(s)
            return ( test_val > 1700 and test_val < 3000)
        except ValueError:
            return False

    @classmethod
    def archive( cls, name="monsoon", type="monsoon" ):
        arrays = {}
        for id in ['AI', 'EPI', 'NEI', 'SPI', 'NCI', 'WPI', 'NMI']:
            td = IITMDataSource( id, type )
            xarray = td.getTimeseries().xr
            arrays[id] = xarray
        dset = xa.Dataset( arrays )
        path = Archive.getExperimentPath( "IITM", name )
        dset.to_netcdf( path, mode="w" )
        print( "Archived IITM data to " + path )
        print( dset )

if __name__ == "__main__":

    IITMDataSource.archive()

