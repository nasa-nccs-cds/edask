from stratus.handlers.manager import handlers
from stratus_endpoint.handler.base import Task, Status
from edas.process.test import TestDataManager as mgr
from stratus.handlers.app import StratusCore
import os
HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join( HERE, "zmq_client_settings.ini" )

if __name__ == "__main__":

    stratus = StratusCore( settings=SETTINGS_FILE )

    client = handlers.getClient( "edas" )

    request = dict(
        domain=[{"name": "d0", "lat": {"start": 50, "end": 55, "system": "values"},
                 "lon": {"start": 40, "end": 42, "system": "values"},
                 "time": {"start": "1980-01-01", "end": "1981-12-31", "crs": "timestamps"}}],
        input=[{"uri": mgr.getAddress("merra2", "tas"), "name": "tas:v0", "domain": "d0"}],
        operation=[ { "epa": "edas.subset", "input": "v0"} ]
    )

    task: Task = client.request( "exe", **request )
    result = task.getResult( block=True )
    print("Received result: " + str(result))
