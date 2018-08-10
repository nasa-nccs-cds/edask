from edask.portal.client import EDASPortalClient

edas_server: str="127.0.0.1"
request_port: int=4556
response_port: int=4557

portal = EDASPortalClient(  edas_server, request_port, response_port )
response_manager = portal.createResponseManager()
domain = {"name":"d0"}

def testOnePointCDSparkAvg():
    datainputs = """[
        domain=[{
            "name":"d0",
            "lat":{
                "start":40.25,
                "end":40.25,
                "system":"values"
            },
            "lon":{
                "start":89.75,
                "end":89.75,
                "system":"values"
            },
            "time":{
                "start":0,
                "end":20,
                "system":"indices"
            },
            "level":{
                "start":0,
                "end":5,
                "system":"indices"
            }
        }],
        variable=[{
            "uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_6hr_hur.ncml",
            "name":"hur",
            "domain":"d0"
        }],
        operation=[{
            "name":"CDSpark.average",
            "input":"hur",
            "domain":"d0",
            "axes":"xy"
        }]
    ]"""
    print('Query:\n' + datainputs)

    rId1 = portal.sendMessage("execute", ["WPS", datainputs, '{ "response":"object" }'])
    response_manager.run()

testOnePointCDSparkAvg()

portal.shutdown()
