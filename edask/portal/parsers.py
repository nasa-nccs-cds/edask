from pyparsing import *

def sval( input: ParseResults ): return "".join( [ str(x) for x in input.asList() ] )
def list2dict( input: ParseResults ): return {elem[0]: elem[1] for elem in input.asList() }
def str2int( input: ParseResults ): return int( sval(input) )
def str2float( input: ParseResults ): return float( sval(input) )

class WpsCwtParser:

    def __init__(self):
        self.datainputs = self.getDatainputsParser()

    def getDatainputsParser(self):
        integer = ( Optional("-") + Word(nums) ).setParseAction(str2int)
        rfloat = ( integer + "." + integer ).setParseAction(str2float)
        efloat = ( rfloat + CaselessKeyword("E") + integer ).setParseAction(str2float)
        numval = integer ^ rfloat ^ efloat
        key = QuotedString( '"' )
        token = key ^ numval
        elem = ( key + Suppress(':') + token + Suppress( ZeroOrMore(",") ) )
        dict = ( Suppress("{") + OneOrMore(Group(elem)) + Suppress("}") ).setParseAction(list2dict)
        specval = dict ^ token
        specelem = (key + Suppress(':') + specval + Suppress(ZeroOrMore(",")))
        spec = ( Suppress("{") + OneOrMore(Group(specelem)) + Suppress("}") )
        specs = (Suppress("[") + delimitedList( spec, delim=',')  + Suppress("]")).setParseAction(list2dict)
        name = Word( alphanums )
        decl = name + Suppress("=") + specs + Suppress(ZeroOrMore(","))
        datainputs = ( Suppress("[") + OneOrMore(Group(decl))  + Suppress("]") ).setParseAction(list2dict)
        return datainputs

    def parseDatainputs(self, datainputs):
        return self.datainputs.parseString(datainputs)[0]

if __name__ == "__main__":
    parser = WpsCwtParser()

    testStr = '[ domain=[{ "name":"d0", "lat":{"start":40.25,  "end":40.25, "system":"values" }, "lon":{ "start":89.75,"end":89.75, "system":"values" }, "time":{ "start":0,"end":20, "system":"indices" }, "level":{ "start":0,     "end":5,     "system":"indices" } }], variable=[{ "uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_6hr_hur.ncml", "name":"hur", "domain":"d0" }], operation=[{ "name":"CDSpark.average", "input":"hur", "domain":"d0","axes":"xy"}]    ]'

    result = parser.parseDatainputs( testStr )

    print( result )
