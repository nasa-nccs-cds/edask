from edas.process.test import LocalTestManager, DistributedTestManager
from typing import List, Optional, Tuple, Dict, Any
from edas.workflow.kernel import EDASDataset
import time
appConf = { "sources.allowed": "collection,https" }
GLOBAL = True

def test_world_clim_mean ( mgr, start_year, nYears=10 ):

    variables = [{"uri": "collection:/cip_merra2_6hr", "name": "tas:temp", "domain": "d0"}, {"uri": "collection://cip_merra2_6hr", "name": "pr:moist", "domain": "d0"}]
#    variables = [{"uri": "collection://merra2_inst1_2d_asm_Nx", "name": "T2M:temp", "domain": "d0"}, {"uri": "collection://merra2_inst1_2d_asm_Nx", "name": "QV2M:moist", "domain": "d0"}]
    operations = [{"name": "edas.worldClim", "input": "temp,moist"}]

    worldClimResults = {}
    for iYear in range(0,nYears):
        base_year = start_year + iYear
        domains = [ {"name": "d0", "time": {"start": f'{base_year}-01-01T00Z', "end": f'{base_year+1}-01-01T00Z', "system": "timestamps"} } ]
        if not GLOBAL:
            domains[0]["lat"] = { "start": 25, "end": 50, "system": "values" }
            domains[0]["lon"] = { "start": 200, "end": 300, "system": "values" }
        print( f"Computing WorldClim fields with domain {domains[0]}")
        results:  List[EDASDataset] = mgr.testExec( domains, variables, operations )
        results[0].save(f"merra2-WorldClim-{base_year}")
        worldClimResults[iYear] =  results[0]

    worldClimResultSum = worldClimResults[0]
    for iYear in range(1, nYears): worldClimResultSum = worldClimResultSum + worldClimResults[iYear]
    (worldClimResultSum/nYears).save( f"cip-merra2-WorldClim-mean-GLOBAL-FOYER-{start_year}-{start_year+nYears}")

if __name__ == "__main__":
#    mgr = LocalTestManager( "PyTest", __file__, appConf )
    mgr = DistributedTestManager( "PyTest",  __file__, { **appConf, "scheduler.address":"explore101:8786" } )
    test_world_clim_mean( mgr, 1990, 1 )


