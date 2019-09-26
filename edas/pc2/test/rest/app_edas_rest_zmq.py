from pc2.app.core import Pc2Core
import os
HERE = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE = os.path.join( HERE, "edas_rest_zmq_settings.ini" )

if __name__ == '__main__':
    core = Pc2Core( SETTINGS_FILE )
    app = core.getApplication( )
    app.run()
