from pyparsing import *
from typing import Sequence, List, Dict, Any

def sval( input: ParseResults ): return "".join( [ str(x) for x in input.asList() ] )
def list2dict( input: ParseResults ): return { elem[0]: elem[1] for elem in input.asList() }
def str2int( input: ParseResults ): return int( sval(input) )
def str2float( input: ParseResults ): return float( sval(input) )

class WpsCwtParser:

    integer = (Optional("-") + Word(nums)).setParseAction(str2int)
    float = (Optional("-") + Word(nums + ".") + Optional(CaselessLiteral("E") + Optional("-") + Word(nums))).setParseAction(str2float)
    numval = integer ^ float
    key = QuotedString('"')
    name = Word(alphanums)
    token = key ^ numval

    def __init__(self):
        self.datainputsParser = self.getDatainputsParser()

    @classmethod
    def getDatainputsParser(cls):
        dict = cls.keymap( cls.key, cls.token )
        spec = cls.keymap( cls.key, dict ^ cls.token )
        return cls.keymap( cls.name, cls.list( spec ), "[]", "=" )

    @classmethod
    def parseDatainputs(cls, datainputs) -> Dict[str,List[Dict[str,Any]]]:
        return cls.getDatainputsParser().parseString(datainputs)[0]

    @staticmethod
    def keymap( key: Token, value: Token, enclosing: str = "{}", sep=":", delim="," ):
        elem = ( key + Suppress(sep) + value + Suppress(ZeroOrMore(delim) ) )
        return ( Suppress(enclosing[0]) + OneOrMore(Group(elem)) + Suppress(enclosing[1]) ).setParseAction( list2dict )

    @staticmethod
    def list( item, enclosing: str = "[]", delim="," ):
        elem = item + Suppress( ZeroOrMore(delim) )
        return ( Suppress(enclosing[0]) + Group(OneOrMore(elem)) + Suppress(enclosing[1]) )


if __name__ == "__main__":

    testStr = '[ domain=[{ "name":"d0",   \n   "lat":{"start":-40.25,  "end":-4.025E1, "system":"values" }, "lon":{ "start":8975e-2,"end":-897.5E-1, "system":"values" }, "time":{ "start":0,"end":20, "system":"indices" }, "level":{ "start":0,     "end":5,     "system":"indices" } }, { "name":"d1", "level":{ "start":0,"end":5, "system":"indices" } }], variable=[{ "uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_6hr_hur.ncml", "name":"hur", "domain":"d0" } ], operation=[{ "name":"CDSpark.average", "input":"hur", "domain":"d0","axes":"xy"}]    ]'

    result = WpsCwtParser.parseDatainputs( testStr )

    print( result )
