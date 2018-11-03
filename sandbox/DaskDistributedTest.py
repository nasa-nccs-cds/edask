from dask.distributed import Client
server = "edaskwndev01:8786"
client = Client( server )

def square(x):
    return x ** 2

def neg(x):
    return -x

A = client.map(square, range(10))
B = client.map(neg, A)
total = client.submit(sum, B)
result = total.result()

print( result )