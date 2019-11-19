from edas.process.test import LocalTestManager
from typing import List, Optional, Tuple, Dict, Any
from edas.workflow.kernel import EDASDataset
import traceback

def world_clim_6hr( mgr: LocalTestManager ):
    try:
        domains = [{ "name":"d0", "time": { "start":'1990-01-01T00Z', "end":'1991-01-01T00Z', "system":"timestamps"  } } ]
        variables = [ { "uri": mgr.getAddress( "merra2-6hr", "tas"), "name":"tas:temp", "domain":"d0" }, { "uri": mgr.getAddress( "merra2-6hr", "pr"), "name":"pr:precip", "domain":"d0" } ]
        operations = [ { "name":"edas.worldClim", "input":"temp,precip"  } ]
        results:  List[EDASDataset] = mgr.testExec( domains, variables, operations )
        results[0].save("cip_merra2_6hr-WorldClim")
    except Exception as ex:
        print( traceback.format_exc() )

def world_clim_mth( mgr: LocalTestManager ):
    try:
        domains = [{ "name":"d0", "time": { "start":'1989-12-01T00Z', "end":'1991-02-01T00Z', "system":"timestamps"  } } ]
        variables = [ { "uri": mgr.getAddress( "merra2", "tasmin"), "name": "tasmin:minTemp", "domain": "d0" },
                      { "uri": mgr.getAddress( "merra2", "tasmax"), "name": "tasmax:maxTemp", "domain": "d0" },
                      { "uri": mgr.getAddress( "merra2", "pr"), "name":"pr:moist", "domain":"d0" } ]
        operations = [ { "name":"edas.worldClim", "input":"minTemp,maxTemp,moist"  } ]
        results:  List[EDASDataset] = mgr.testExec( domains, variables, operations )
        results[0].save("merra2_mth-WorldClim")
    except Exception as ex:
        print( traceback.format_exc() )


if __name__ == "__main__":
    appConf = {"sources.allowed": "collection,https", "log.metrics": "true"}
    mgr = LocalTestManager("PyTest", "world_clim", appConf)
    world_clim_mth( mgr )