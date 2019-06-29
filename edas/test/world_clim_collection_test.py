from edas.process.test import LocalTestManager, DistributedTestManager
import time
appConf = { "sources.allowed": "collection,https" }
LOCAL_TESTS = True

def test_world_clim (mgr ):

    domains = [{"name": "d0", "lat": {"start": 35, "end": 45, "system": "values"},
                "lon": {"start": 240, "end": 260, "system": "values"},
                "time": {"start": '1990-01-01T00Z', "end": '1991-01-01T00Z', "system": "timestamps"}}]

    variables = [{"uri": "collection://merra2-6hr", "name": "tas:temp", "domain": "d0"},
                 {"uri": "collection://merra2-6hr", "name": "pr:precip", "domain": "d0"}]

    operations = [{"name": "edas.worldClimTest", "input": "temp,precip"}]

    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)

if __name__ == "__main__":
    mgr = LocalTestManager( "PyTest", __file__, appConf ) if LOCAL_TESTS else DistributedTestManager( "PyTest",  __file__, { **appConf, "scheduler.address":"edaskwndev01:8786" } )
    test_world_clim( mgr )


