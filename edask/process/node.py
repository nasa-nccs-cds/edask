from typing import  List, Dict, Any, Sequence, Union, Optional, Iterator, Set
import re

class Node:

    def __init__( self, name: str, _metadata: Dict[str,Any] = {} ):
       self._name = name
       self.metadata: Dict[str,Any] = _metadata

    @property
    def name(self)->str: return self._name

    def getParm(self, key: str, default: Any = None ) -> Any:
        return self.metadata.get( key, default )

    def findParm(self, idmatch: str, default: Any = None ) -> Any:
        found = [value for id, value in self.metadata.items() if re.match(idmatch,id) ]
        return found[0] if len(found) else default

    def getParms(self, keys: List[str] ) -> Dict[str,Any]:
        return dict( filter( lambda item: item[0] in keys, self.metadata.items() ) )

    def getMetadata(self, ignore: List[str] ) -> Dict[str,Any]:
        return { key:val for key,val in self.metadata.items() if key not in ignore }

    def __getitem__( self, key: str ) -> Any: return self.metadata.get( key )
    def __setitem__(self, key: str, value: Any ): self.metadata[key] = value
