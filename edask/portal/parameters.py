from os.path import dirname, abspath, join
import logging, os
from typing import Sequence, List, Dict, Mapping, Optional

class ParameterManager:

    def __init__(self):
        from edask import CONFIG_DIR
        self.logger =  logging.getLogger()
        self.path = join( CONFIG_DIR,"app.conf" )
        self._parms: Dict[str,str] = self.getAppConfiguration()

    @property
    def parms(self)-> Dict[str,str]: return self._parms

    def getAppConfiguration(self) ->  Dict[str,str]:
        appConfig: Dict[str,str] = {}
        try:
            config_FILE = open( self.path )
            for line in config_FILE.readlines():
                toks = line.split("=")
                if len( toks ) == 2: appConfig[toks[0].strip()] = toks[1].strip()
        except Exception as err:
            self.logger.warning( "Can't load app config file 'app.conf' from config dir: " + self.path )
            self.logger.warning( str(err) )
        return appConfig

    def __getitem__( self, key: str ) -> str: return self._parms.get( key )
    def get( self, key: str, default=None ) -> str: return self._parms.get( key, default )

ParmMgr =   ParameterManager()

if __name__ == '__main__':
    print( ParmMgr['dap.engine'] )


