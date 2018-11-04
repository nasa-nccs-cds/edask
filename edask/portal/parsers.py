from pyparsing import *
from typing import Sequence, List, Dict, Any
import logging, string, random
from edask.util.logging import EDASLogger

def sval( input: ParseResults ): return "".join( [ str(x) for x in input.asList() ] )
def list2dict( input: ParseResults ): return { elem[0]: elem[1] for elem in input.asList() }
def str2int( input: ParseResults ): return int( sval(input) )
def str2float( input: ParseResults ): return float( sval(input) )

class SizeParser:

    @classmethod
    def parse(cls, input: str) -> int:
        input1 = input.lower().strip()
        if input1[-1] == 't': return int( input1[:-1] ) * 1000000000000
        elif input1[-1] == 'g': return int( input1[:-1] ) * 1000000000
        elif input1[-1] == 'm': return int( input1[:-1] ) * 1000000
        elif input1[-1] == 'k': return int( input1[:-1] ) * 1000
        else: return int( input1 )

class WpsCwtParser:
    logger = EDASLogger.getLogger()

    integer = (Optional("-") + Word(nums)).setParseAction(str2int)
    float = (Optional("-") + Word(nums + ".") + Optional(CaselessLiteral("E") + Optional("-") + Word(nums))).setParseAction(str2float)
    numval = integer ^ float
    key = QuotedString('"')
    name = Word(alphanums)
    token = key ^ numval
    delim = Word(",") ^ Word(";")
    vsep = Word("|") ^ Word(":")

    @classmethod
    def getDatainputsParser(cls):
        dict = cls.keymap( cls.key, cls.token )
        spec = cls.keymap( cls.key, dict ^ cls.token ^ cls.list( cls.token ) )
        return cls.keymap( cls.name, cls.list( spec ), "[]", "=" )

    @classmethod
    def getOpConnectionsParser(cls):
        output = Suppress(cls.vsep) + cls.name
        input = cls.seq( cls.name )
        item = input + Optional(Group(output))
        return cls.seq( Group(item) )

    @classmethod
    def parseDatainputs(cls, datainputs) -> Dict[str,List[Dict[str,Any]]]:
        try:
            return cls.getDatainputsParser().parseString(datainputs)[0]
        except ParseException as err:
            cls.logger.error("\n\n -----> Error parsing input at col {}: '{}'\n".format(err.col, err.line))
            raise err

    @classmethod
    def parseOpConnections(cls, opConnections) -> List[List[List[str]]]:
        try:
            opCon = ",".join( opConnections ) if hasattr(opConnections, '__iter__') else opConnections
            return cls.getOpConnectionsParser().parseString( str(opCon) )[0]
        except ParseException as err:
            cls.logger.error( "\n\n -----> Error parsing input at col {}: '{}'\n".format( err.col, err.line ) )
            raise err

    @classmethod
    def keymap( cls, key: Token, value: Token, enclosing: str = "{}", sep=":" ):
        elem = ( key + Suppress(sep) + value + Suppress(ZeroOrMore(cls.delim) ) )
        return ( Suppress(enclosing[0]) + OneOrMore(Group(elem)) + Suppress(enclosing[1]) ).setParseAction( list2dict )

    @classmethod
    def list( cls, item, enclosing: str = "[]" ):
        elem = item + Suppress( ZeroOrMore(cls.delim) )
        return ( Suppress(enclosing[0]) + Group(OneOrMore(elem)) + Suppress(enclosing[1]) )

    @classmethod
    def seq( cls, item ):
        elem = item + Suppress( ZeroOrMore(cls.delim) )
        return  Group(OneOrMore(elem))

    @classmethod
    def postProcessResult( cls, result: Dict[str,List[Dict[str,Any]]] ) ->  Dict[str,List[Dict[str,Any]]]:
        # for key, decls in result.items():
        #     for decl in decls:
        #         print(".")
        return result

    @staticmethod
    def get( altKeys: List[str], spec: Dict[str,Any] ) -> Any:
        for key in altKeys:
            value = spec.get( key, None )
            if value is not None: return value
        return None

    @staticmethod
    def split( sepKeys: List[str], value: str ) -> List[str]:
        for sep in sepKeys:
            if sep in value:
                return value.split(sep)
        return [ value ]

    @staticmethod
    def randomStr(length) -> str:
      tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
      return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

if __name__ == '__main__':

    # datainputs0 = """[
    # variable = [{"domain": "d0", "uri": "https://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP//reanalysis/MERRA2/mon/atmos/tas.ncml", "id": "tas|19543b"}];
    # domain = [{"id": "d0", "time": {"start": "1980-01-01T00:00:00Z", "step": 1, "end": "1980-12-31T23:59:00Z", "crs": "timestamps"}}];
    # operation = [{"input": ["19543b"], "domain": "d0", "axes": "tyx", "name": "CDSpark.ave", "result": "96af34"}]
    #                     ]"""
    # result = WpsCwtParser.parseDatainputs( datainputs0 )
    # print( str(result) )

    opConnections = "a,c,b"
    result = WpsCwtParser.parseOpConnections( opConnections )
    print( str(result) )

