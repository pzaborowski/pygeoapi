import requests
from pygeoapi.provider.base import ProviderNoDataError, ProviderQueryError
from pygeoapi.provider.base_edr import BaseEDRProvider
from pygeoapi.provider.xarray_ import _to_datetime_string, XarrayProvider
import logging

LOGGER = logging.getLogger(__name__)

class WCS2EDRApiProvider(BaseEDRProvider, XarrayProvider):
    def __init__(self, provider_def):
        super().__init__(provider_def)
        self.base_url = provider_def['base_url']
        #self.layer = provider_def['layer']
        #self.format = provider_def['format']


    def get_coverage(self, coverage_id, bbox, time=None):
        params = {
            'service': 'WCS',
            'version': '2.0.1',
            'request': 'GetCoverage',
            'coverageid': coverage_id,
            'format': 'application/netcdf',
            'subset': f'Lat({bbox[1]},{bbox[3]})&subset=Long({bbox[0]},{bbox[2]})'
        }
        if time is not None:
            params['subset'] += f'&subset=ansi("{time}")'

        response = requests.get(self.base_url, params=params)
        if response.status_code != 200:
            return None

        data = response.content
        
        out_meta = {
            'bbox': [
                data.coords[self.x_field].values[0],
                data.coords[self.y_field].values[0],
                data.coords[self.x_field].values[-1],
                data.coords[self.y_field].values[-1]
            ],
            "time": [
                _to_datetime_string(data.coords[self.time_field].values[0]),
                _to_datetime_string(data.coords[self.time_field].values[-1])
            ],
            "driver": "xarray",
            "height": data.dims[self.y_field],
            "width": data.dims[self.x_field],
            "time_steps": data.dims[self.time_field],
            "time_values": data.coords['TIME'].values,
            "variables": {var_name: var.attrs
                          for var_name, var in data.variables.items()}
        }
        return self.gen_covjson(out_meta, data, self.fields)
        # Convert data to CoverageJSON format
        """         coverage = {
            'type': 'Coverage',
            'domain': {
                'type': 'Domain',
                'domainType': 'Grid',
                'axes': {
                    'x': {'values': [bbox[0], bbox[2]]},
                    'y': {'values': [bbox[1], bbox[3]]}
                }
            },
            'parameters': {
                'data': {
                    'type': 'NdArray',
                    'dataType': 'float',
                    'values': data
                }
            }
        } """

    @BaseEDRProvider.register()
    def cube(self, **kwargs):
        """
            Extract data from collection

            :param query_type: query type
            :param bbox: `list` of minx,miny,maxx,maxy coordinate values as `float`
            :param datetime_: temporal (datestamp or extent)
            :param select_properties: list of parameters
            :param z: vertical level(s)
            :param format_: data format of output

            :returns: coverage data as dict of CoverageJSON or native format
            """

        query_params = {}

        LOGGER.debug(f'Query parameters: {kwargs}')

        LOGGER.debug(f"Query type: {kwargs.get('query_type')}")
        bbox = kwargs.get('bbox')
        if len(bbox) == 4:
            self.bbox = bbox
        else:
            raise ProviderQueryError('z-axis not supported')

        LOGGER.debug('Processing parameter-name')
        select_properties = kwargs.get('select_properties')

            # example of fetching instance passed
            # TODO: apply accordingly
        instance = kwargs.get('instance')
        LOGGER.debug(f'instance: {instance}')

        datetime_ = kwargs.get('datetime_')
        if datetime_ is not None:
            query_params[self._coverage_properties['time_axis_label']] = datetime_  # noqa
        
        layer = kwargs.get('select_properties')
        if layer:
            self.layer = kwargs.get('select_properties')

        LOGGER.debug(f'query parameters: {query_params}')


        response = self.get_coverage(self.layer, bbox, datetime_)
        return response

    """         if datetime_:
                time_str = datetime_.strftime('%Y-%m-%dT%H:%M:%SZ')
            else:
                time_str = ''
            url = f"{self.base_url}/wcs?request=GetCoverage&service=WCS&version=2.0.1&coverageId={self.layer}&format={self.format}&subset=Lat({bbox[1]},{bbox[3]})&subset=Long({bbox[0]},{bbox[2]})&subset=ansi({time_str})"
            LOGGER.debug("querying " + url)
            response = requests.get(url)
            if response.status_code != 200:
                return None """

        
    def get_fields(self):
        """
        Get provider field information (names, types)

        :returns: dict of dicts of parameters
        """

        return self.get_coverage_rangetype()
    
    def query(self, startindex=0, limit=10, resulttype='results', bbox=[], datetime_=None, properties=[], sortby=[]):
        if resulttype != 'results':
            return None
        coverage_data = self.get_coverage(bbox, datetime_)
        if coverage_data is None:
            return None
        return {'type': 'FeatureCollection',
                'features': [{'type': 'Feature',
                              'geometry': {'type': 'Polygon',
                                           'coordinates': [[[bbox[0], bbox[1]],
                                                            [bbox[0], bbox[3]],
                                                            [bbox[2], bbox[3]],
                                                            [bbox[2], bbox[1]],
                                                            [bbox[0], bbox[1]]]]},
                              'properties': {'coverage_data': coverage_data}}]}
