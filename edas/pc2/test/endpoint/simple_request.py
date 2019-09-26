import json

requestSpec = dict(
    domain=[ dict( name="d0", time=dict(start="1980-01-01", end="2001-12-31", crs="timestamps")) ],
    input=[dict(uri="collection:merra2", name="tas:v1", domain="d1")],
    operation=[ dict(epa="edas.ave", axis="yt", input="v1", result="v1ave"), dict(epa="edas.diff", axis="xy", input=["v1","v1ave"]) ]
)

print( json.dumps( requestSpec ) )