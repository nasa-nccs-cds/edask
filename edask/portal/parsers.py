from pyparsing import *
from typing import Sequence, List, Dict, Any
import logging

def sval( input: ParseResults ): return "".join( [ str(x) for x in input.asList() ] )
def list2dict( input: ParseResults ): return { elem[0]: elem[1] for elem in input.asList() }
def str2int( input: ParseResults ): return int( sval(input) )
def str2float( input: ParseResults ): return float( sval(input) )

class WpsCwtParser:
    logger = logging.getLogger()

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
        cls.logger.info( "WpsCwtParser-> Parsing: " + datainputs )
        return cls.getDatainputsParser().parseString(datainputs)[0]

    @staticmethod
    def keymap( key: Token, value: Token, enclosing: str = "{}", sep=":", delim="," ):
        elem = ( key + Suppress(sep) + value + Suppress(ZeroOrMore(delim) ) )
        return ( Suppress(enclosing[0]) + OneOrMore(Group(elem)) + Suppress(enclosing[1]) ).setParseAction( list2dict )

    @staticmethod
    def list( item, enclosing: str = "[]", delim="," ):
        elem = item + Suppress( ZeroOrMore(delim) )
        return ( Suppress(enclosing[0]) + Group(OneOrMore(elem)) + Suppress(enclosing[1]) )
