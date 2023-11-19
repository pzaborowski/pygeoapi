# =================================================================
#
# Authors: Piotr Zaborowski
#
# Copyright (c) 2023 OGC
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
from pygeoapi.provider.influx_ import InfluxDBProvider
from pygeoapi.provider.xarray_ import _to_datetime_string, XarrayProvider
import pandas as pd

LOGGER = logging.getLogger(__name__)


class InfluxEDRProvider(BaseEDRProvider, InfluxDBProvider):
    """EDR Provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.rasterio_.RasterioProvider
        """

        BaseEDRProvider.__init__(self, provider_def)

    def get_fields(self):
        """
        Get provider field information (names, types)

        :returns: dict of dicts of parameters
        """
        return self.get_coverage_rangetype()


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
        wkt = self.read_position_parameters(kwargs, query_params)

        LOGGER.debug('Processing parameter-name')
        select_properties = kwargs.get('select_properties')

        # example of fetching instance passed
        # TODO: apply accordingly
        instance = kwargs.get('instance')
        LOGGER.debug(f'instance: {instance}')

        datetime_ = self._read_datetime_qparam(kwargs, query_params)

        LOGGER.debug(f'query parameters: {query_params}')

        LOGGER.debug("Querying the data from database")
        '''cube = Polygon(((10.0,58.0),(12.0,58.0),(12.0,59.0),(10.0,59.0),(10.0,58.0)))'''
        '''
        try:
            if select_properties:
                self.fields = select_properties
                data = self._data[[*select_properties]]
            else:
                data = self._data
            if (datetime_ is not None and
                isinstance(query_params[self.time_field], slice)): # noqa
                # separate query into spatial and temporal components
                LOGGER.debug('Separating temporal query')
                time_query = {self.time_field:
                              query_params[self.time_field]}
                remaining_query = {key: val for key,
                                   val in query_params.items()
                                   if key != self.time_field}
                data = data.sel(time_query).sel(remaining_query,
                                                method='nearest')
            else:
                data = data.sel(query_params, method='nearest')
        except KeyError:
            raise ProviderNoDataError()

        try:
            height = data.dims[self.y_field]
        except KeyError:
            height = 1
        try:
            width = data.dims[self.x_field]
        except KeyError:
            width = 1
        time, time_steps, time_values = self._parse_time_metadata(data, kwargs)
        bbox = wkt.bounds
        out_meta = {
            'bbox': [bbox[0], bbox[1], bbox[2], bbox[3]],
            "time": time,
            "driver": "xarray",
            "height": height,
            "width": width,
            "time_steps": time_steps,
            "time_values": time_values,
            #"time_values": list(map(lambda dt: str(dt), data.coords[self.time_field].to_numpy())),
            "variables": {var_name: var.attrs
                          for var_name, var in data.variables.items()}
        }
'''
        return self.gen_covjson(out_meta, data, self.fields)

    def _read_datetime_qparam(self, kwargs, query_params):
        datetime_ = kwargs.get('datetime_')
        if datetime_ is not None:

            if '/' in datetime_:
                dts = datetime_.split('/')
                query_params[self._coverage_properties['time_axis_label']] = slice(pd.to_datetime(dts[0]),
                                                                                   pd.to_datetime(dts[1]))  # noqa
            else:
                query_params[self._coverage_properties['time_axis_label']] = datetime_  # noqa
        return datetime_

    def _read_position_qparam(self, kwargs, query_params):
        LOGGER.debug(f"Query type: {kwargs.get('query_type')}")
        wkt = kwargs.get('wkt')
        if wkt is not None:
            LOGGER.debug('Processing WKT')
            LOGGER.debug(f'Geometry type: {wkt.type}')
            if wkt.type == 'Point':
                query_params[self._coverage_properties['x_axis_label']] = wkt.x
                query_params[self._coverage_properties['y_axis_label']] = wkt.y
            elif wkt.type == 'LineString':
                query_params[self._coverage_properties['x_axis_label']] = wkt.xy[0]  # noqa
                query_params[self._coverage_properties['y_axis_label']] = wkt.xy[1]  # noqa
            elif wkt.type == 'Polygon':
                query_params[self._coverage_properties['x_axis_label']] = slice(wkt.bounds[0], wkt.bounds[2])  # noqa
                query_params[self._coverage_properties['y_axis_label']] = slice(wkt.bounds[1], wkt.bounds[3])  # noqa
                pass
        return wkt

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

        self._read_bbox_qparam(kwargs, query_params)

        LOGGER.debug('Processing parameter-name')
        select_properties = _read_select_parameters_qparam(kwargs)

        # example of fetching instance passed
        # TODO: apply accordingly
        instance = kwargs.get('instance')
        LOGGER.debug(f'instance: {instance}')

        datetime_ = self._read_datetime_qparam(kwargs, query_params)

        LOGGER.debug(f'query parameters: {query_params}')

        try:
            if select_properties:
                self.fields = select_properties
                data = self._data[[*select_properties]]
            else:
                data = self._data
            data = data.sel(query_params)
        except KeyError:
            raise ProviderNoDataError()

        height = data.dims[self.y_field]
        width = data.dims[self.x_field]
        time, time_steps, time_values = self._parse_time_metadata(data, kwargs)

        out_meta = {
            'bbox': [
                data.coords[self.x_field].values[0],
                data.coords[self.y_field].values[0],
                data.coords[self.x_field].values[-1],
                data.coords[self.y_field].values[-1]
            ],
            "time": time,
            "driver": "xarray",
            "height": height,
            "width": width,

            "time_steps": data.dims[self.time_field],
            "time_values": list(map(lambda dt: str(dt), data.coords[self.time_field].to_numpy())),

            "variables": {var_name: var.attrs
                          for var_name, var in data.variables.items()}
        }

        return self.gen_covjson(out_meta, data, self.fields)
    def _read_select_properties_qparam(self, kwargs, query_params):
        sp = kwargs.get('select_properties')


    def _read_bbox_qparam(self, kwargs, query_params):
        bbox = kwargs.get('bbox')
        if len(bbox) == 4:
            query_params['cube'] = Polygon(((bbox[0], bbox[1]), (bbox[2], bbox[1]), (bbox[2], bbox[3]), (bbox[0], bbox[3]), (bbox[0], bbox[1])))
        else:
            raise ProviderQueryError('z-axis not supported in queries')

    def _make_datetime(self, datetime_):
        """
        Make xarray datetime query

        :param datetime_: temporal (datestamp or extent)

        :returns: xarray datetime query
        """
        datetime_ = datetime_.rstrip('Z').replace('Z/', '/')
        if '/' in datetime_:
            begin, end = datetime_.split('/')
            if begin == '..':
                begin = self._data[self.time_field].min().values
            if end == '..':
                end = self._data[self.time_field].max().values
            if np.datetime64(begin) < np.datetime64(end):
                return [begin, end]
            else:
                LOGGER.debug('Reversing slicing from high to low')
                return [end, begin]
        else:
            return [datetime_, datetime_]

    def gen_covjson(self):
        """ serialise to covjson representation
            potentially to be replaced byt the formatter
        """
