from os.path import dirname, abspath, join
import logging, os
from typing import Sequence, List, Dict, Mapping, Optional
from edas.util.logging import EDASLogger

class ParameterManager:

    def __init__(self):
        self.logger =  EDASLogger.getLogger()
        self.EDAS_CONFIG_DIR = os.environ.get('EDAS_CONFIG_DIR',os.path.expanduser("~/.edas/conf" ) )
        assert os.path.isdir( self.EDAS_CONFIG_DIR ), f"Error, the EDAS configuration directory '{self.EDAS_CONFIG_DIR}' does not exist"
        self.path = os.path.expanduser( os.path.join( self.EDAS_CONFIG_DIR, "app.conf" ) )
        assert os.path.isfile( self.path ), f"Error, the EDAS configuration file '{self.path}' does not exist.  Copy edas/resourses/app.conf.template to '{self.path}' and edit."
        aliases = { "wps.server.address": "client.address", "scheduler.address": "dask.scheduler" }
        self._parms: Dict[str,str] = self.getAppConfiguration( aliases )
        self.TRANSIENTS_DIR = self._parms.get( "edas.transients.dir", "/tmp" )
        self.COLLECTIONS_DIR = self._parms.get("edas.coll.dir", "~/.edas" )
        for cpath in [self.TRANSIENTS_DIR, self.COLLECTIONS_DIR]:
            if not os.path.exists(cpath): os.makedirs(cpath)

    def update(self, parms: Dict[str,str] = None, **kwargs ):
        self._parms.update( parms if parms else {}, **kwargs )
        self.logger.info( f"@PM-> Update parms: {self._parms}")

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
    def getBool( self, key: str, default: bool ) -> bool:
        rv = self._parms.get( key, None )
        if rv is None: return default
        return rv.lower().startswith("t")

EdasEnv = ParameterManager()

if __name__ == '__main__':
    print(EdasEnv['dap.engine'])




