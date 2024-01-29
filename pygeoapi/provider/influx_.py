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
    """InfluxDB abstact provider, query but not serialise"""

    def __init__(self, provider_def):
        """
        Initialize object reading all config fields:
         - db api connection and test connection with simple query
         - measurment 'tables' with

        :param provider_def: provider definition

        :returns: pygeoapi.provider.csv_.CSVProvider
        """

        super().__init__(provider_def)
        self.ifc_token = os.environ.get(provider_def['data']['token_variable'])
        self.ifc_bucket = provider_def['data']['bucket']
        self.ifc_url = provider_def['data']['url']
        self.locations = provider_def['locations']
        self.parameters_def = provider_def['parameters']
        self.rangetype = None
        self.rangetype = self.get_coverage_rangetype()
        # Store the URL of your InfluxDB instance
        # self.geometry_x = provider_def['geometry']['x_field']
        # self.geometry_y = provider_def['geometry']['y_field']
        # self.fields = self.get_fields()

    def get_coverage_rangetype(self, *args, **kwargs):
        """
        Provide coverage rangetype

        :returns: CIS JSON object of rangetype metadata
        """
        if self.rangetype:
            return self.rangetype

        LOGGER.debug('building parameters rangeset')
        rangetype = {
            'type': 'DataRecord',
            'field': []
        }
        LOGGER.debug('building parameters self.parameters: ' + str(self.parameters_def))
        for l in self.locations:
            for table_name in l['tables'].keys():
                LOGGER.debug('building parameters rangeset for table: ' + str(table_name))
                variables = l['tables'][table_name]
                LOGGER.debug('building parameters rangeset for keys: ' + str(variables))
                for p_name in variables.keys():
                    LOGGER.debug('building parameters rangeset: ' + p_name)
                    try:
                        parameter = self.parameters_def[p_name]
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
        LOGGER.debug("rangetype: " + str(rangetype))
        return rangetype

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
        Throw("This class is abstract, serialisation implementation in ")

    def define_query(self, bucket, measurement, start_date, end_date):
        # Set up query, in this example data since start_date to end_date from table {measurement}
        # :bucket: database name string
        # :measurement: name of table containing time series
        # :start_date: startdate as string, e.g. '2023-05-01T00:00:00Z'
        # :end_date: end_date as string, e.g. '2023-05-02T00:00:00Z'
        # returns query string in InfluxDbClient form£at
        range_stmt = ""
        if start_date is not None and end_date is not None:
            range_stmt = f'''|> range(start: {start_date}Z , stop: {end_date}Z)'''
        elif start_date is not None:
            range_stmt = f'''|> range(start:-{start_time})'''
        elif end_date is not None:
            range_set = f'''|> range(stop: {end_date})'''

        query = f'''from(bucket:"{bucket}")''' + \
                range_stmt + \
                f'''|> filter(fn:(r) => r._measurement == "{measurement}")
                 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''
        return query

    def query_to_df(self, url, token, query):
        # queries database at url with query and returns pandas DataFrame
        # :url: url to database
        # :token: username & password
        # :query: query string in InfluxDbClient format
        # returns: pandas DataFrame

        LOGGER.debug("influx query: " + query)

        with ifc.InfluxDBClient(url=url, token=token) as client:
            df = client.query_api().query_data_frame(query)

        # LOGGER.debug("influx query result: " + df)
        return df
