from edas.process.test import LocalTestManager

def world_clim( mgr: LocalTestManager ):
    domains = [{ "name":"d0",   "lat":  { "start":35, "end":45,  "system":"values" },
                                "lon":  { "start":240, "end":260, "system":"values" },
                                "time": { "start":'1990-01-01T00Z', "end":'1991-01-01T00Z', "system":"timestamps"  } } ]
    variables = [ { "uri": mgr.getAddress( "merra2-6hr", "tas"), "name":"tas:temp", "domain":"d0" }, { "uri": mgr.getAddress( "merra2-6hr", "pr"), "name":"pr:precip", "domain":"d0" } ]
    operations = [ { "name":"edas.worldClim", "input":"temp,precip"  } ]
    results = mgr.testExec( domains, variables, operations )
    mgr.print(results)


if __name__ == "__main__":
    appConf = {"sources.allowed": "collection,https", "log.metrics": "true"}
    mgr = LocalTestManager("PyTest", "world_clim", appConf)
    world_clim( mgr )