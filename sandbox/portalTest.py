from edas.portal.client import EDASPortalClient
from edas.process.test import LocalTestManager

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
            "uri": "collection:cip_merra2_mth",
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

    rId1 = portal.sendMessage( "execute", ["WPS", datainputs.replace(" ", "").replace("\n", "") ] )
    portal.waitUntilDone()



if __name__ == '__main__':

    testAvg()

