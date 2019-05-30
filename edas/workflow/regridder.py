import numbers, re
import xarray as xa
import cdms2
from edas.util.logging import EDASLogger
from edas.workflow.data import EDASArray

def old_div(a, b):
    if isinstance(a, numbers.Integral) and isinstance(b, numbers.Integral): return a // b
    else: return a / b

logger = EDASLogger.getLogger()

class Regridder:

    @classmethod
    def parse_uniform_arg( cls, value, default_start, default_n ):
        result = re.match('^(\\d\\.?\\d?)$|^(-?\\d\\.?\\d?):(\\d\\.?\\d?):(\\d\\.?\\d?)$', value)
        if result is None: raise Exception(f'Failed to parse uniform argument {value}')

        groups = result.groups()
        if groups[1] is None:
            delta = int(groups[0])
            default_n = old_div( default_n, delta )
        else:
            default_start = int(groups[1])
            default_n = int(groups[2])
            delta = int(groups[3])

        start = default_start + (delta / 2.0)
        return start, default_n, delta

    @classmethod
    def align( cls, source: EDASArray, target: EDASArray ) -> xa.DataArray:
        v0: cdms2.tvariable.TransientVariable = source.xrArray.to_cdms2()
        v1: cdms2.tvariable.TransientVariable = target.xrArray.to_cdms2()
        v2 = v0.regrid( v1.getGrid() )
        return xa.DataArray.from_cdms2(v2)

    @classmethod
    def regrid( cls, source: "EDASArray", gridSpec: str ) -> xa.DataArray:
        try: grid_type, grid_param = gridSpec.split('~')
        except Exception: raise Exception(f'Error generating grid "{gridSpec}"')

        v0: cdms2.tvariable.TransientVariable = source.xrArray.to_cdms2()
        ( xaxis, yaxis ) = ( v0.getLongitude(), v0.getLatitude() )
        grid = cls.generate_user_defined_grid(grid_type, grid_param, (yaxis[0], yaxis[-1]), (xaxis[0], xaxis[-1]) )
        v2 = v0.regrid( grid )
        if grid_type.lower() == 'gaussian': v2 = v2.subRegion( latitude=(yaxis[0], yaxis[-1], 'co'), longitude=(xaxis[0], xaxis[-1]) )
        return xa.DataArray.from_cdms2(v2)

    @classmethod
    def generate_user_defined_grid(cls, grid_type, grid_param, lat_bounds = None, lon_bounds = None ):
        logger.info('Generating grid %r %r', grid_type, grid_param)

        if grid_type.lower() == 'uniform':
            result = re.match('^(.*)x(.*)$', grid_param)
            if result is None: raise Exception( f'Failed to parse uniform configuration from {grid_param}' )
            lat_start = -90.0 if lat_bounds is None else lat_bounds[0]
            lat_extent = 180.0 if lat_bounds is None else lat_bounds[1] - lat_bounds[0]
            lon_start = 0.0 if lon_bounds is None else lon_bounds[0]
            lon_extent = 360.0 if lon_bounds is None else lon_bounds[1] - lon_bounds[0]
            start_lat, nlat, delta_lat = cls.parse_uniform_arg(result.group(1), lat_start, lat_extent )
            start_lon, nlon, delta_lon = cls.parse_uniform_arg(result.group(2), lon_start, lon_extent )
            grid = cdms2.createUniformGrid(start_lat, nlat, delta_lat, start_lon, nlon, delta_lon)

            logger.info('Created target uniform grid {} from lat {}:{}:{} lon {}:{}:{}'.format( grid.shape, start_lat, delta_lat, nlat, start_lon, delta_lon, nlon))
        elif grid_type.lower() == 'gaussian':
            try:
                nlats = int(grid_param)
            except ValueError:
                raise Exception('Error converting gaussian parameter to an int')

            grid = cdms2.createGaussianGrid(nlats)
            logger.info(f'Created target gaussian grid {grid.shape}')
        else:
            raise Exception(f'Unknown grid type for regridding: {grid_type}' )

        return grid
