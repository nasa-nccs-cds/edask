from os.path import dirname, abspath, join
from typing import Sequence, List, Dict, Mapping, Optional

class ParameterManager:

    def __init__(self):
        baseDir = dirname(dirname(dirname(abspath(__file__))))
        self.path = join( baseDir, "resources", "parameters" )
        self._parms: Dict[str,str] = self.parse()

    def parse(self) -> Dict[str,str]:
        parms: Dict[str,str] = {}
        parm_file = open( self.path )
        for line in parm_file.readlines():
            toks = line.split("=")
            if len( toks ) > 1:
                parms[ toks[0].strip() ] = toks[1].strip()
        return parms

    def __getitem__( self, key: str ) -> str: return self._parms.get( key )
    def get( self, key: str, default=None ) -> str: return self._parms.get( key, default )

ParmMgr =   ParameterManager()

if __name__ == '__main__':
    print( ParmMgr['dap.engine'] )


