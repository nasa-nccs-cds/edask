from edas.process.test import LocalTestManager
from typing import List, Optional, Tuple, Dict, Any
from edas.workflow.kernel import EDASDataset
import traceback

def world_clim_6hr( mgr: LocalTestManager, year: int ):
    try:
        domains = [{ "name":"d0", "time": { "start":f'{year-1}-12-01T00Z', "end":f'{year+1}-02-01T00Z', "system":"timestamps"  } } ]
        variables = [ { "uri": mgr.getAddress( "merra2-6hr", "tas"), "name":"tas:temp", "domain":"d0" }, { "uri": mgr.getAddress( "merra2-6hr", "pr"), "name":"pr:precip", "domain":"d0" } ]
        operations = [ { "name":"edas.worldClim", "input":"temp,precip"  } ]
        results:  List[EDASDataset] = mgr.testExec( domains, variables, operations )
        results[0].save(f"cip_merra2-6hr-WorldClim-{year}")
    except Exception as ex:
        print( traceback.format_exc() )

def world_clim_mth( mgr: LocalTestManager, range ):
    try:
        domains = [ { "name":"d0", "time": { "start":f'{range[0]-1}-12-01T00Z', "end":f'{range[1]+1}-02-01T00Z', "system":"timestamps" } } ]
        variables = [ { "uri": mgr.getAddress( "merra2", "tasmin"), "name": "tasmin:minTemp", "domain": "d0" },
                      { "uri": mgr.getAddress( "merra2", "tasmax"), "name": "tasmax:maxTemp", "domain": "d0" },
                      { "uri": mgr.getAddress( "merra2", "huss"), "name":"huss:moist", "domain":"d0" } ]
        operations = [ { "name":"edas.worldClim", "input":"minTemp,maxTemp,moist", 'tscale':10.0, 'hscale':1.0e5 } ]
        results:  List[EDASDataset] = mgr.testExec( domains, variables, operations )
        results[0].save(f"merra2-mth-WorldClim_{range[0]}-{range[1]}")
    except Exception as ex:
        print( traceback.format_exc() )


if __name__ == "__main__":
    range = [ 1990, 1999 ]
    appConf = {"sources.allowed": "collection,https", "log.metrics": "true"}
    mgr = LocalTestManager( "PyTest", "world_clim", appConf )
    world_clim_mth( mgr, range )