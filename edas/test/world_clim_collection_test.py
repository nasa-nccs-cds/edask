from edas.process.test import LocalTestManager, DistributedTestManager
from typing import List, Optional, Tuple, Dict, Any
from edas.workflow.kernel import EDASDataset
import time
appConf = { "sources.allowed": "collection,https" }
LOCAL_TESTS = False
GLOBAL = True

def test_world_clim (mgr ):

    domains = [ {"name": "d0", "time": {"start": '1990-01-01T00Z', "end": '2000-01-01T00Z', "system": "timestamps"}, "lev": {"start": 0, "end": 0, "system": "indices"} } ]

    if not GLOBAL:
        domains[0]["lat"] = {"start": 25, "end": 50, "system": "values"}
        domains[0]["lon"] = {"start": 200, "end": 300, "system": "values"}

    variables = [{"uri": "collection://cip_merra2_6hr", "name": "tas:temp", "domain": "d0"},
                 {"uri": "collection://cip_merra2_6hr", "name": "hus:moist", "domain": "d0"}]

    operations = [{"name": "edas.worldClim", "input": "temp,moist"}]

    results:  List[EDASDataset] = mgr.testExec( domains, variables, operations )
    results[0].save( "cip_merra2_6hr-WorldClim")

if __name__ == "__main__":
    mgr = LocalTestManager( "PyTest", __file__, appConf ) if LOCAL_TESTS else DistributedTestManager( "PyTest",  __file__, { **appConf, "scheduler.address":"edaskwndev01:8786" } )
    test_world_clim( mgr )


