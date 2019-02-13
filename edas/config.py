from os.path import dirname, abspath, join
import logging, os
from typing import Sequence, List, Dict, Mapping, Optional
from edas.util.logging import EDASLogger

class ParameterManager:

    def __init__(self):
        self.logger =  EDASLogger.getLogger()
        assert os.path.isdir( os.path.expanduser("~/.edas" ) ), "Error, the EDAS configuration directory '~/.edas' does not exist"
        self.path = os.path.expanduser("~/.edas/conf/app.conf" )
        assert os.path.isfile( self.path ), "Error, the EDAS configuration file '{}' does not exist.  Copy edas/resourses/app.conf.template to '{}' and edit.".format( self.path, self.path )
        aliases = { "wps.server.address": "client.address" }
        self._parms: Dict[str,str] = self.getAppConfiguration( aliases )
        self.CONFIG_DIR = self._parms.get( "edas.cache.dir", os.path.expanduser("~/.edas/conf") )
        self.COLLECTIONS_DIR = self._parms.get("edas.coll.dir", self.CONFIG_DIR )
        for cpath in [ self.CONFIG_DIR, self.COLLECTIONS_DIR ]:
            if not os.path.exists(cpath): os.makedirs(cpath)

    def update(self, parms: Dict[str,str] = None, **kwargs ):
        self._parms.update( parms if parms else {}, **kwargs )

    @property
    def parms(self)-> Dict[str,str]: return self._parms

    def getAppConfiguration(self, aliases: Dict[str,str] ) ->  Dict[str,str]:
        appConfig: Dict[str,str] = {}
        try:
            config_FILE = open( self.path )
            for line in config_FILE.readlines():
                toks = line.split("=")
                if len( toks ) == 2: appConfig[toks[0].strip()] = toks[1].strip()
            for key,value in aliases.items():
                result = appConfig.get( value )
                if result and not appConfig.get(key):
                    appConfig[key] = result
        except Exception as err:
            self.logger.warning( "Can't load app config file 'app.conf' from config dir: " + self.path )
            self.logger.warning( str(err) )
        return appConfig

    def __getitem__( self, key: str ) -> str: return self._parms.get( key )
    def get( self, key: str, default=None ) -> str: return self._parms.get( key, default )
    def getBool( self, key: str, default: bool = None ) -> bool:
        rv = self._parms.get( key, None )
        if rv is None: return default
        return rv.lower().startswith("t")

EdaskEnv = ParameterManager()

if __name__ == '__main__':
    print(EdaskEnv['dap.engine'])




