import sys, numpy as np

class MessageParser:

    def getIntArg( self, index, default ): return int(sys.argv[index]) if index < len( sys.argv ) else default

    def s2m( self, mdataStr ):
        metadata = {}
        for item in mdataStr.split(";"):
            toks = item.split(":")
            if len(toks) > 1:
                metadata[ toks[0] ] = ":".join(toks[1:])
        return metadata

    def s2ia(self, mdataStr ):
        return np.asarray( [ int(item) for item in mdataStr.split(',') ] )

    def s2it(self, mdataStr ):
        return tuple( [ int(item) for item in mdataStr.split(',') ] )

    def sa2s(self,  strArray ): return ','.join( strArray )

    def ia2s(self,  intArray ): return ','.join( str(e) for e in intArray )

    def null2s(self,  val ):
        if val is not None: return str(val);
        else: return "";

    def m2s(self,  map  ): return ';'.join( key+":"+self.null2s(value) for key,value in map.iteritems() )

    def s2b(self,  s ):
        if s.lower() == "t": return True
        else: return False

mParse = MessageParser()
