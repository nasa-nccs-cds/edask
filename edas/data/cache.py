from edas.workflow.data import EDASArray
from typing import Dict
from collections import OrderedDict
from edas.config import EdasEnv
from edas.portal.parsers import SizeParser

class CacheManager:

    def __init__(self):
        self.arrayCache: Dict[str,EDASArray] = OrderedDict()
        self.maxSize = SizeParser.parse(EdasEnv.get("cache.size.max", "500M"))
        self.currentSize = 0

    def cache(self, id: str, variable: EDASArray ):
        assert id not in self.arrayCache,  "Error: id {} already exists in cache".format( id )
        input_size = variable.bsize
        assert input_size <= self.maxSize, "Error: array {} is too big for cache".format( id )
        self.clearSpace( input_size )
        self.arrayCache[id] = variable
        self.currentSize += input_size

    def clearSpace( self, bsize: int ):
        if self.currentSize + bsize > self.maxSize:
            for key, value in self.arrayCache.items():
               del self[key]
               if self.currentSize + bsize < self.maxSize:  break


    def __getitem__( self, key: str ) -> EDASArray: return self.arrayCache.get( key, None )
    def __setitem__( self, key: str, value: EDASArray ): self.cache( key, value )

    def __delitem__( self, key ):
        value = self.arrayCache[key]
        del self.arrayCache[key]
        self.currentSize = self.currentSize - value.bsize

EDASKCacheMgr = CacheManager()
