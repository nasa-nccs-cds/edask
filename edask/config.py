from os.path import dirname, abspath, join
import logging, os
from typing import Sequence, List, Dict, Mapping, Optional
from edask.util.logging import EDASLogger

class ParameterManager:

    def __init__(self):
        self.logger =  EDASLogger.getLogger()
        self.path = os.path.expanduser("~/.edask/conf/app.conf" )
        self._parms: Dict[str,str] = self.getAppConfiguration()
        self.CONFIG_DIR = self._parms.get( "edask.cache.dir", os.path.expanduser("~/.edask/conf") )
        self.COLLECTIONS_DIR = self._parms.get("edask.coll.dir", self.CONFIG_DIR )
        for cpath in [ self.CONFIG_DIR, self.COLLECTIONS_DIR ]:
            if not os.path.exists(cpath): os.makedirs(cpath)

    def update(self, parms: Dict[str,str] = None, **kwargs ):
        self._parms.update( parms if parms else {}, **kwargs )

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

EdaskEnv = ParameterManager()

if __name__ == '__main__':
    print(EdaskEnv['dap.engine'])




