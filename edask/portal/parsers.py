from pyparsing import *

def sval( input: ParseResults ): return "".join( [ str(x) for x in input.asList() ] )
def list2dict( input: ParseResults ): return {elem[0]: elem[1] for elem in input.asList() }
def aslist( input: ParseResults ): return { input.asList() }
def str2int( input: ParseResults ): return int( sval(input) )
def str2float( input: ParseResults ): return float( sval(input) )
def keymap( key: Token, value: Token, enclosing: str = "{}", sep=":", delim="," ):
    elem = (key + Suppress(sep) + value + Suppress(ZeroOrMore(delim)))
    return ( Suppress(enclosing[0]) + OneOrMore(Group(elem)) + Suppress(enclosing[1]) ).setParseAction( list2dict )
def list( item, enclosing: str = "[]", delim="," ):
    return (Suppress(enclosing[0]) + delimitedList( item, delim=delim )  + Suppress(enclosing[1]))

class WpsCwtParser:

    def __init__(self):
        self.datainputsParser = self.getDatainputsParser()

    def getDatainputsParser(self):

        integer = ( Optional("-") + Word(nums) ).setParseAction(str2int)
        float = ( Optional("-") + Word( nums + ".Ee" ) ).setParseAction(str2float)
        numval = integer ^ float
        key = QuotedString( '"' )
        name = Word( alphanums )
        token = key ^ numval

        dict = keymap( key, token )
        spec = keymap( key, dict ^ token )
        return keymap( name, list( spec ), "[]", "=" )

    def parseDatainputs(self, datainputs):
        return self.datainputsParser.parseString(datainputs)[0]

if __name__ == "__main__":
    parser = WpsCwtParser()

    testStr = '[ domain=[{ "name":"d0",   \n   "lat":{"start":-40.25,  "end":-4.025E1, "system":"values" }, "lon":{ "start":8.975e1,"end":89.75, "system":"values" }, "time":{ "start":0,"end":20, "system":"indices" }, "level":{ "start":0,     "end":5,     "system":"indices" } }, { "name":"d1", "level":{ "start":0,"end":5, "system":"indices" } }], variable=[{ "uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_6hr_hur.ncml", "name":"hur", "domain":"d0" } ], operation=[{ "name":"CDSpark.average", "input":"hur", "domain":"d0","axes":"xy"}]    ]'

    result = parser.parseDatainputs( testStr )

    print( result )
