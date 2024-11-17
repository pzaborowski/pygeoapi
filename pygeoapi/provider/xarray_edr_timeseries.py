# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#
# Copyright (c) 2022 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging

import numpy as np

from pygeoapi.provider.base import ProviderNoDataError, ProviderQueryError
from pygeoapi.provider.base_edr import BaseEDRProvider
from pygeoapi.provider.xarray_edr import XarrayEDRProvider
from pygeoapi.provider.xarray_ import (
    _to_datetime_string,
    _convert_float32_to_float64,
    XarrayProvider,
)

LOGGER = logging.getLogger(__name__)


class XarrayEDRTimeSeriesProvider(XarrayEDRProvider):
    """EDR Provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.rasterio_.RasterioProvider
        """

        BaseEDRProvider.__init__(self, provider_def)
        XarrayProvider.__init__(self, provider_def)
        self._context = provider_def.get('context')
        self._lat = provider_def.get('lat')
        self._lon = provider_def.get('lon')
        self._domainType = provider_def.get('domainType')

    @BaseEDRProvider.register()
    def position(self, **kwargs):
        """
        Extract data from collection collection

        :param query_type: query type
        :param wkt: `shapely.geometry` WKT geometry
        :param datetime_: temporal (datestamp or extent)
        :param select_properties: list of parameters
        :param z: vertical level(s)
        :param format_: data format of output

        :returns: coverage data as dict of CoverageJSON or native format
        """

        query_params = {}

        LOGGER.debug(f'Query parameters: {kwargs}')

        LOGGER.debug(f"Query type: {kwargs.get('query_type')}")

        wkt = kwargs.get('wkt')
        if wkt is not None:
            LOGGER.debug('Processing WKT')
            LOGGER.debug(f'Geometry type: {wkt.geom_type}')
            if wkt.geom_type == 'Point':
                query_params[self._coverage_properties['x_axis_label']] = wkt.x
                query_params[self._coverage_properties['y_axis_label']] = wkt.y
            elif wkt.geom_type == 'LineString':
                query_params[self._coverage_properties['x_axis_label']] = wkt.xy[0]  # noqa
                query_params[self._coverage_properties['y_axis_label']] = wkt.xy[1]  # noqa
            elif wkt.geom_type == 'Polygon':
                query_params[self._coverage_properties['x_axis_label']] = slice(wkt.bounds[0], wkt.bounds[2])  # noqa
                query_params[self._coverage_properties['y_axis_label']] = slice(wkt.bounds[1], wkt.bounds[3])  # noqa
                pass

        LOGGER.debug('Processing parameter-name')
        select_properties = kwargs.get('select_properties')

        # example of fetching instance passed
        # TODO: apply accordingly
        instance = kwargs.get('instance')
        LOGGER.debug(f'instance: {instance}')

        datetime_ = kwargs.get('datetime_')
        if datetime_ is not None:
            query_params[self.time_field] = self._make_datetime(datetime_)

        LOGGER.debug(f'query parameters: {query_params}')

        try:
            if select_properties:
                self._fields = {k: v for k, v in self._fields.items() if k in select_properties}  # noqa
                data = self._data[[*select_properties]]
            else:
                data = self._data

            if self.time_field in query_params:
                remaining_query = {
                    key: val for key, val in query_params.items()
                    if key != self.time_field
                }
                if isinstance(query_params[self.time_field], slice):
                    time_query = {
                        self.time_field: query_params[self.time_field]
                    }
                else:
                    time_query = {
                        self.time_field: (
                                data[self.time_field].dt.date ==
                                query_params[self.time_field]
                        )
                    }
                data = data.sel(
                    time_query).sel(remaining_query, method='nearest')
            else:
                data = data.sel(query_params, method='nearest')
        except KeyError:
            raise ProviderNoDataError()

        try:
            height = data.sizes[self.y_field]
        except KeyError:
            height = 1
        try:
            width = data.sizes[self.x_field]
        except KeyError:
            width = 1
        time, time_steps = self._parse_time_metadata(data, kwargs)

        bbox = wkt.bounds
        out_meta = {
            'bbox': [bbox[0], bbox[1], bbox[2], bbox[3]],
            "time": time,
            "driver": "xarray",
            "height": height,
            "width": width,
            "time_steps": time_steps,
            "variables": {var_name: var.attrs
                          for var_name, var in data.variables.items()}
        }

        return self.gen_covjson(out_meta, data, self.fields)



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


        _data = self._trim_data(kwargs)

        out_meta = {
            "driver": "xarray",
            "variables": {var_name: var.attrs
                          for var_name, var in _data.variables.items()}
        }
        if self._lat & self._lon:
            out_meta["bbox"] = [self._lon[0],self._lat[0],self._lon[-1],self._lat[-1]]
        if _data.variables[self.x_field] & _data.variables[self.y_field]:
            out_meta["bbox"] = [
                _data.variables[self.x_field].values[0],
                _data.coords[self.y_field].values[0],
                _data.coords[self.x_field].values[-1],
                _data.coords[self.y_field].values[-1]
                ]
            
        if _data.sizes.get[self.y_field]:
            out_meta["height"] = _data.sizes.get[self.y_field]
        else:
            out_meta["height"] = 1
        if _data.sizes.get[self.x_field]:
            out_meta["width"] = _data.sizes.get[self.x_field]
        else:
            out_meta["width"] = 1
        
        out_meta["time"], out_meta["time_steps"] = self._parse_time_metadata(_data, kwargs)


        #return self.gen_covjson(out_meta, data, self.fields)
        ts = _data.sizes.get('timeSeries')
        if ts & ts > 1:
            json = {"type": "CoverageCollection",
                    "domain": self._domainType}
            for t in _data.
        else:
            json = {"type": "Coverage",
                    "domain": self._domainType}
        
        return json

    def _trim_data(self, kwargs):
        LOGGER.debug(f'Query parameters: {kwargs}')

        LOGGER.debug(f"Query type: {kwargs.get('query_type')}")

        bbox = kwargs.get('bbox')
        xmin, ymin, xmax, ymax = self._configure_bbox(bbox)

        if len(bbox) == 4:
            query_params[self.x_field] = slice(bbox[xmin], bbox[xmax])
            query_params[self.y_field] = slice(bbox[ymin], bbox[ymax])
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
            query_params[self.time_field] = self._make_datetime(datetime_)

        LOGGER.debug(f'query parameters: {query_params}')
        try:
            if select_properties:
                self._fields = {k: v for k, v in self._fields.items() if k in select_properties}  # noqa
                data = self._data[[*select_properties]]
            else:
                data = self._data
            data = data.sel(query_params)
            data = _convert_float32_to_float64(data)
        except KeyError:
            raise ProviderNoDataError()

        return data


    def _parse_time_metadata(self, data, kwargs):
        """
        Parse time information for output metadata.

        :param data: xarray dataset
        :param kwargs: dictionary

        :returns: list of temporal extent, number of timesteps
        """
        try:
            time = self._get_time_range(data)
        except KeyError:
            time = []
        try:
            time_steps = data.coords[self.time_field].size
        except KeyError:
            time_steps = kwargs.get('limit')
        return time, time_steps

    def _configure_bbox(self, bbox):
        xmin, ymin, xmax, ymax = 0, 1, 2, 3
        if self._data[self.x_field][0] > self._data[self.x_field][-1]:
            xmin, xmax = xmax, xmin
        if self._data[self.y_field][0] > self._data[self.y_field][-1]:
            ymin, ymax = ymax, ymin
        return xmin, ymin, xmax, ymax


    def get_fields(self):
        LOGGER.debug('XarrayProvider get_fields')
        if not self._fields:
            for key, value in self._data.variables.items():
                if key not in self._data.coords:
                    LOGGER.debug('Adding variable')
                    dtype = value.dtype
                    if dtype.name.startswith('float'):
                        dtype = 'float'
                    elif dtype.name.startswith('int'):
                        dtype = 'integer'

                    self._fields[key] = {
                        'type': dtype,
                        'title': value.attrs.get('long_name'),
                        'x-ogc-unit': value.attrs.get('units')
                    }

        return self._fields

def _get_coverage_properties(self):
        """
        Helper function to normalize coverage properties
        :param provider_def: provider definition

        :returns: `dict` of coverage properties
        """

        time_var, y_var, x_var = [None, None, None]

        for coord in self._data.coords:
            if coord.lower() == 'time':
                time_var = coord
                continue
            if self._data.coords[coord].attrs.get('units') == 'degrees_north':
                y_var = coord
                continue
            if self._data.coords[coord].attrs.get('units') == 'degrees_east':
                x_var = coord
                continue

        if self.x_field is None:
            self.x_field = x_var
        if self.y_field is None:
            self.y_field = y_var
        if self.time_field is None:
            self.time_field = time_var

        # It would be preferable to use CF attributes to get width
        # resolution etc but for now a generic approach is used to assess
        # all of the attributes based on lat lon vars

        properties = {
            'bbox': [
                self._data.coords[self.x_field].values[0],
                self._data.coords[self.y_field].values[0],
                self._data.coords[self.x_field].values[-1],
                self._data.coords[self.y_field].values[-1],
            ],
            'bbox_crs': 'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'crs_type': 'GeographicCRS',
            'x_axis_label': self.x_field,
            'y_axis_label': self.y_field,
            'width': self._data.sizes.get(self.x_field),
            'height': self._data.sizes.get(self.y_field),
            'bbox_units': 'degrees'
            }
        if len(self._data.coords[self.x_field]) > 1:
            properties['resx'] = np.abs(
                self._data.coords[self.x_field].values[1]
                - self._data.coords[self.x_field].values[0]
            )
        if len(self._data.coords[self.y_field]) > 1:
            properties['resx'] = np.abs(
                self._data.coords[self.y_field].values[1]
                - self._data.coords[self.y_field].values[0]
            )

        if self.time_field is not None:
            properties['time_axis_label'] = self.time_field
            properties['time_range'] = [
                _to_datetime_string(
                    self._data.coords[self.time_field].values[0]
                    ),
                _to_datetime_string(
                    self._data.coords[self.time_field].values[-1]
                    ),
            ]
            properties['time'] = self._data.sizes[self.time_field]
            properties['time_duration'] = self.get_time_coverage_duration()
            properties['restime'] = self.get_time_resolution()

        # Update properties based on the xarray's CRS
        epsg_code = self.storage_crs.to_epsg()
        LOGGER.debug(f'{epsg_code}')
        if epsg_code == 4326 or self.storage_crs == 'OGC:CRS84':
            pass
            LOGGER.debug('Confirmed default of WGS 84')
        else:
            properties['bbox_crs'] = \
                f'https://www.opengis.net/def/crs/EPSG/0/{epsg_code}'
            properties['inverse_flattening'] = \
                self.storage_crs.ellipsoid.inverse_flattening
            if self.storage_crs.is_projected:
                properties['crs_type'] = 'ProjectedCRS'

        LOGGER.debug(f'properties: {properties}')

        properties['axes'] = [
            properties['x_axis_label'],
            properties['y_axis_label']
        ]

        if self.time_field is not None:
            properties['axes'].append(properties['time_axis_label'])

        return properties


json = {
            "type" : "Coverage",
            "domain" : {
                "type" : "Domain",
                "domainType" : "PointSeries",
                "axes": {
                "x" : { "values": [-10.1] },
                "y" : { "values": [ -40.2] },
                "t" : { "values": ["2013-01-01","2013-01-02","2013-01-03",
                                    "2013-01-04","2013-01-05","2013-01-06"] }
                },
                "referencing": [{
                "coordinates": ["x","y"],
                "system": {
                    "type": "GeographicCRS",
                    "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
                }
                }, {
                "coordinates": ["t"],
                "system": {
                    "type": "TemporalRS",
                    "calendar": "Gregorian"
                }
                }]
            },
            "parameters" : {
                "PSAL": {
                "type" : "Parameter",
                "description" : {
                    "en": "The measured salinity, in practical salinity units (psu) of the sea water "
                },
                "unit" : {
                    "symbol" : "psu"
                },
                "observedProperty" : {
                    "id" : "http://vocab.nerc.ac.uk/standard_name/sea_water_salinity/",
                    "label" : {
                    "en": "Sea Water Salinity"
                    }
                }
                },
                "POTM": {
                "type" : "Parameter",
                "description" : {
                    "en": "The potential temperature, in degrees celcius, of the sea water"
                },
                "unit" : {
                    "label": {
                    "en": "Degree Celsius"
                    },
                    "symbol": {
                    "value": "Cel",
                    "type": "http://www.opengis.net/def/uom/UCUM/"
                    }
                },
                "observedProperty" : {
                    "id" : "http://vocab.nerc.ac.uk/standard_name/sea_water_potential_temperature/",
                    "label" : {
                    "en": "Sea Water Potential Temperature"
                    }
                }
                }
            },
            "ranges" : {
                "PSAL" : {
                "type" : "NdArray",
                "dataType": "float",
                "axisNames": ["t"],
                "shape": [6],
                "values" : [ 43.9599, 43.9599, 43.9640, 43.9640, 43.9679, 43.9879 ]
                },
                "POTM" : {
                "type" : "NdArray",
                "dataType": "float",
                "axisNames": ["t"],
                "shape": [6],
                "values" : [ 23.8, 23.7, 23.9, 23.4, 23.2, 22.4 ]
                }
            }
            }

