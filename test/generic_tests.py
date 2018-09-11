from datetime import datetime
import re

def timeDelta( dateStr: str, offset: str ) -> datetime:
    from dateutil.parser import parse
    from dateutil.relativedelta import relativedelta
    result: datetime = parse( dateStr )
    timeToks = offset.split(",")
    for timeTok in timeToks:
        try:
            m = re.search(r'[0-9]*',timeTok)
            val = int(m.group(0))
            units = timeTok[len(m.group(0)):].strip().lower()
            if units.startswith("y"): result =  result+relativedelta(years=+val)
            elif units.startswith("mi"): result =  result+relativedelta(minutes=+val)
            elif units.startswith("m"): result =  result+relativedelta(months=+val)
            elif units.startswith("d"): result =  result+relativedelta(days=+val)
            elif units.startswith("w"): result =  result+relativedelta(weeks=+val)
            elif units.startswith("h"): result =  result+relativedelta(hours=+val)
            elif units.startswith("s"): result =  result+relativedelta(seconds=+val)
            else: raise Exception()
        except Exception:
            raise Exception( "Parse error in offset specification, should be like: '5y,3m,6d'")
    return result

dateStr="2003-10-25"
offset="14d"

print( timeDelta(dateStr,offset) )