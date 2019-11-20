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

def world_clim_mth( mgr: LocalTestManager, year: int ):
    try:
        domains = [{ "name":"d0", "time": { "start":f'{year-1}-12-01T00Z', "end":f'{year+1}-02-01T00Z', "system":"timestamps"  } } ]
        variables = [ { "uri": mgr.getAddress( "merra2", "tasmin"), "name": "tasmin:minTemp", "domain": "d0" },
                      { "uri": mgr.getAddress( "merra2", "tasmax"), "name": "tasmax:maxTemp", "domain": "d0" },
                      { "uri": mgr.getAddress( "merra2", "pr"), "name":"pr:moist", "domain":"d0" } ]
        operations = [ { "name":"edas.worldClim", "input":"minTemp,maxTemp,moist"  } ]
        results:  List[EDASDataset] = mgr.testExec( domains, variables, operations )
        results[0].save(f"merra2-mth-WorldClim-{year}")
    except Exception as ex:
        print( traceback.format_exc() )


if __name__ == "__main__":
    year = 2000
    appConf = {"sources.allowed": "collection,https", "log.metrics": "true"}
    mgr = LocalTestManager("PyTest", "world_clim", appConf)
    world_clim_mth( mgr, year )