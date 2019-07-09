from edas.process.test import LocalTestManager, DistributedTestManager
from typing import List, Optional, Tuple, Dict, Any
from edas.workflow.kernel import EDASDataset
import time
appConf = { "sources.allowed": "collection,https" }
LOCAL_TESTS = True

def test_world_clim (mgr ):

    domains = [{"name": "d0", "time": {"start": '1990-01-01T00Z', "end": '1991-02-01T00Z', "system": "timestamps"}}]
    variables = [{"uri": "collection://cip_merra2_6hr", "name": "tas:temp", "domain": "d0"},
                 {"uri": "collection://cip_merra2_6hr", "name": "pr:precip", "domain": "d0"}]
    operations = [{"name": "edas.worldClimTest", "input": "temp,precip"}]

    results:  List[EDASDataset] = mgr.testExec( domains, variables, operations )
    mgr.print(results)
    results[0].save( "cip_merra2_6hr-WorldClim")

if __name__ == "__main__":
    mgr = LocalTestManager( "PyTest", __file__, appConf ) if LOCAL_TESTS else DistributedTestManager( "PyTest",  __file__, { **appConf, "scheduler.address":"edaskwndev01:8786" } )
    test_world_clim( mgr )


