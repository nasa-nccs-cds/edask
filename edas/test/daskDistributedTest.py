from dask.distributed import Client
from edas.util.logging import EDASLogger
import time
local = True

if local:
    print( "Local dask server ")
    client = Client()
else:
    server = "localhost:8786"
    client = Client( server )

time.sleep( 20 )

def square(x):
    logger = EDASLogger.getLogger()
    logger.info( "Executing square: " + str(x))
    return x ** 2

def neg(x):
    return -x

A = client.map(square, range(10))
B = client.map(neg, A)

total = client.submit(sum, B)
result = total.result()

print( result )
