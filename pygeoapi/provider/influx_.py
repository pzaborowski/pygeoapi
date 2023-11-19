# =================================================================
#
# Authors: Piotr Zaborowski <pastich@gmail.com>
#
# Copyright (c) 2022 OGC
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

from collections import OrderedDict
import os
import itertools
import logging
import influxdb_client as ifc
from influxdb_client.client.write_api import SYNCHRONOUS

from pygeoapi.provider.base import (BaseProvider, ProviderQueryError,
                                    ProviderItemNotFoundError)
from pygeoapi.util import get_typed_value, crs_transform

LOGGER = logging.getLogger(__name__)


class InfluxDBProvider(BaseProvider):
    """InfluxDB provider"""

    def __init__(self, provider_def):
        """
        Initialize object reading all config fields:
         - db api connection and test connection with simple query
         - measurment 'tables' with

        :param provider_def: provider definition

        :returns: pygeoapi.provider.csv_.CSVProvider
        """

        super().__init__(provider_def)
        self.token = os.environ.get(provider_def['data']['token_variable'])
        self.ifc_bucket = provider_def['data']['bucket']
        self.ifc_url = provider_def['data']['url']
        self.locations = provider_def['locations']
        self.parameters = provider_def['parameters']

        self.rangetype = self.get_coverage_rangetype()
        # Store the URL of your InfluxDB instance
        #self.geometry_x = provider_def['geometry']['x_field']
        #self.geometry_y = provider_def['geometry']['y_field']
        #self.fields = self.get_fields()

    def get_coverage_rangetype(self, *args, **kwargs):
        """
        Provide coverage rangetype

        :returns: CIS JSON object of rangetype metadata
        """
        LOGGER.debug('building parameters rangeset')
        rangetype = {
            'type': 'DataRecord',
            'field': []
        }
        for l in self.locations:
            for table_name in l['tables'].keys():
                LOGGER.debug('building parameters rangeset for table: ' + str(table_name))
                variables = l['tables'][table_name]
                LOGGER.debug('building parameters rangeset for keys: ' + str(variables))
                for p_name in variables.keys():
                    LOGGER.debug('building parameters rangeset: ' + p_name)
                    try:
                        parameter = self.parameters[p_name]
                    except:
                        continue
                    LOGGER.debug('parameters rangeset parameter name will be: ' + parameter['name'])
                    try:
                        next(field for field in rangetype['field'] if field["id"] == parameter['name'])
                        continue
                    except:
                        LOGGER.debug('adding new parameters rangeset parameter: ' + parameter['name'])
                        rangetype['field'].append({
                            'id': parameter['name'],
                            'type': parameter['name'],
                            'name': parameter['description'],
                            'encodingInfo': {
                                'dataType': parameter['data_type']
                            },
                            'nodata': 'null',
                            'uom': {
                                'id': str(parameter['unit_type']) + parameter['unit_symbol'],
                                'type': parameter['unit_type'],
                                'code': parameter['unit_label']
                            },
                            '_meta': {
                                'tags': 'None'
                            }
                        })
        return rangetype


    @crs_transform
    def query(self, offset=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None, **kwargs):
        """
        CSV query

        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)

        :returns: dict of GeoJSON FeatureCollection
        """

        return self._load(offset, limit, resulttype,
                          properties=properties,
                          select_properties=select_properties,
                          skip_geometry=skip_geometry)

    @crs_transform
    def get(self, identifier, **kwargs):
        """
        query by id

        :param identifier: feature id

        :returns: dict of single GeoJSON feature
        """
        item = self._load(identifier=identifier)
        if item:
            return item
        else:
            err = f'item {identifier} not found'
            LOGGER.error(err)
            raise ProviderItemNotFoundError(err)

    def __repr__(self):
        return f'<InfluxDBProvider> {self.data}'

    def gen_covjson(self):
        Throw("not")
