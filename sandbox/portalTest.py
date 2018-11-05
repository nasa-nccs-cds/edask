from edask.portal.client import EDASPortalClient
from edask.process.test import LocalTestManager

edas_server: str="127.0.0.1"
request_port: int=4556
response_port: int=4557

portal = EDASPortalClient(  edas_server, request_port, response_port )

def testAvg():
    datainputs = """[
        domain=[{
            "name":"d0",
            "lat":{
                "start":40,
                "end":60,
                "system":"values"
            },
            "lon":{
                "start":80,
                "end":90,
                "system":"values"
            },
            "time":{
                "start":0,
                "end":20,
                "system":"indices"
            },
        }],
        variable=[{
            "uri": "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml",
            "name":"tas:v0",
            "domain":"d0"
        }],
        operation=[{
            "name":"xarray.ave",
            "input":"v0",
            "domain":"d0",
            "axes":"xy"
        }]
    ]"""

    print('Query:\n' + datainputs)

    rId1 = portal.sendMessage("execute", ["WPS", datainputs.replace(" ", "").replace("\n", ""), '{ "response":"object" }'])
    portal.waitUntilDone()



if __name__ == '__main__':

    testAvg()

