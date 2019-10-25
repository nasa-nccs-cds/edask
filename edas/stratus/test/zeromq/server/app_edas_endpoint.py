from stratus.handlers.zeromq.app import StratusApp
from stratus.app.core import StratusCore
import os
HERE: str = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE: str = os.path.join( HERE, "generic_zmq_settings.ini" )

if __name__ == '__main__':
    if __name__ == "__main__":
        core: StratusCore = StratusCore(SETTINGS_FILE)
        app: StratusApp = core.getApplication()
        app.run()
