import influxdb_client
import os
from collections import OrderedDict
from flask import Flask, jsonify
import json
import os
import sys

sys.path.append(os.path.abspath("/Users/piotr/repos/pzaborowski/pygeoapi/pygeoapi/pygeoapi/formatter"))
from covjson_utils import *

token = os.environ.get('SINTEF_influx_token')
url = "https://oceanlab.azure.sintef.no:8086"
bucket = "oceanlab"
baseURI = "https://sintef.twin-ocean.eu/data/"


def query_to_df(url, token, query):
    # queries database at url with query and returns pandas DataFrame
    # :url: url to database
    # :token: username & password
    # :query: query string in InfluxDbClient format
    # returns: pandas DataFrame

    with influxdb_client.InfluxDBClient(url=url, token=token) as client:
        df = client.query_api().query_data_frame(query)
    return df


def define_query_since(bucket, measurement, start_time, offset, max):
    # Set up query, in this example data since today-{timespan} from table {measurement}
    # :bucket: database name string
    # :measurement: name of table containing time series
    # :timespan: startdate as string, e.g. '2023-05-01T00:00:00Z'
    # returns query string in InfluxDbClient format

    query = f'''from(bucket:"{bucket}")
 |> range(start:-{start_time})
 |> filter(fn:(r) => r._measurement == "{measurement}")
 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''

    return query


def define_query(bucket, measurement, start_date, end_date):
    # Set up query, in this example data since start_date to end_date from table {measurement}
    # :bucket: database name string
    # :measurement: name of table containing time series
    # :start_date: startdate as string, e.g. '2023-05-01T00:00:00Z'
    # :end_date: end_date as string, e.g. '2023-05-02T00:00:00Z'
    # returns query string in InfluxDbClient form£at
    if start_date is not None and end_date is not None:
        query = f'''from(bucket:"{bucket}")
                 |> range(start: {start_date} , stop: {end_date})
                 |> filter(fn:(r) => r._measurement == "{measurement}")
                 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''
    elif start_date is not None:
        query = f'''from(bucket:"{bucket}")
                 |> range(start:-{start_time})
                 |> filter(fn:(r) => r._measurement == "{measurement}")
                 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''
    elif end_date is not None:
        query = f'''from(bucket:"{bucket}")
                 |> range(stop: {end_date})
                 |> filter(fn:(r) => r._measurement == "{measurement}")
                 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''
    else:
        query = f'''from(bucket:"{bucket}")
                 |> filter(fn:(r) => r._measurement == "{measurement}")
                 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''

    return query


def define_query_interval2(bucket, measurement, start_date, end_date):

    query = f'''from(bucket:"{bucket}")
 |> range(start: {start_date} , stop: {end_date})
 |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''
    return query


data_table = 'ctd_temperature_munkholmen'
result_column = 'temperature'

query = define_query(bucket, data_table, '2023-05-01T02:25:00Z', '2023-05-01T02:30:00Z')
query = f'''from(bucket:"oceanlab")
|> range(start: 2023-05-01T02:30:00Z , stop: 2023-05-01T12:00:00Z)
|> filter(fn:(r) => r._measurement == "ctd_temperature_munkholmen")
|> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")'''
print("query: " + str(query))
data = query_to_df(url, token, query)


# query = define_query_interval2(bucket, data_table, '2023-05-01T00:00:00Z', '2023-05-01T01:00:00Z')
# d2 = query_to_df(url, token, query)

# print(data.columns)
# cols = data.columns
# feature_collection = {
#     "type": "FeatureCollection",
#     "features": []
# }


def set_property(feature, prop, value):
    feature["properties"][prop] = value


def set_observation_feature_properties(resultTime, phenomenonTime, madeBySensor, hasFeatureOfInterest,
                                       observedProperty, wasOriginatedBy, deployedSystem, result, unit):
    set_property(feature, "resultTime", str(resultTime))
    set_property(feature, "phenomenonTime", str(phenomenonTime))
    set_property(feature, "madeBySensor", madeBySensor)
    set_property(feature, "hasFeatureOfInterest", hasFeatureOfInterest)
    set_property(feature, "observedProperty", observedProperty)
    set_property(feature, "wasOriginatedBy", wasOriginatedBy)
    set_property(feature, "deployedSystem", deployedSystem)
    set_property(feature, "unit", unit)
    set_property(feature, "value", result)
    # set_property(feature, "hasResult", {"unit": unit, "value": result})


def build_features_collection():
    for i in data.index:
        feature = {"type": "Feature",
                   "id": 1,
                   # str(baseURI) + data["platform"][i] + "_" + data["edge_device"][i] + "_" + data["sensor"][i],
                   "geometry": "None"
                               '''{
                    "type":"Point",
                    "coordinates":[
                        -75.0,
                        45.0
                    ]
                }''', "properties": OrderedDict()}

        set_observation_feature_properties(data["_time"][i],
                                           data["_time"][i],
                                           str(baseURI) + "/" + data["platform"][i] + "/" + data["edge_device"][i]
                                           + "/" + data["sensor"][i],
                                           str(baseURI) + "/Thing/" + data["platform"][i],
                                           "http://vocab.nerc.ac.uk/standard_name/sea_surface_temperature/",
                                           "UNKNOWN maybe clock",
                                           data["platform"][i],
                                           data[result_column][i],
                                           data["unit"][i]
                                           )
        feature["properties"] = dict(feature["properties"])
        feature_collection["features"].append(feature)

    # feature_collection["numberMatched"] = len(feature_collection["features"])

    print(json.dumps(feature_collection))

    with open("/Users/piotr/Temp/sintef_sst_sample.json", "w") as outfile:
        outfile.write(json.dumps(feature_collection))
    # $sed 's/\'/\"/' sintef_sst_sample.json > sintef_sst_sample2.json
    # gpq convert ~/Temp/sintef_sst_sample2.json ~/Temp/sintef_sst_sample.parquet


# coveragejson with time series for munkholmen, one property

y_values = [58.9355841]
x_values = [11.6646684]
t_values = data['_time']
m_values = data['temperature']
a_values = data['approved']
from datetime import datetime, timezone
from pydantic import AwareDatetime
from covjson_pydantic.coverage import Coverage, CoverageCollection
from covjson_pydantic.domain import Domain, Axes, ValuesAxis, DomainType
from covjson_pydantic.ndarray import NdArray
from covjson_pydantic.parameter import Parameter
from covjson_pydantic.unit import Unit, Symbol
from covjson_pydantic.observed_property import ObservedProperty, Category
from covjson_pydantic.reference_system import ReferenceSystem, ReferenceSystemConnectionObject
from covjson_pydantic.i18n import *
from pydantic import ConfigDict


class CategoryStyled(Category):
    preferredColor: str


approved_map = {"yes": 1, "no": 0}
from shapely.geometry import Point

locations = [{
    "name": "munkholmen", "geometry": {"point": (11.6646684, 58.9355841)}, "tables": {
        'ctd_temperature_munkholmen': {'sea_water_temperature': 'temperature', 'approved': 'approved'},
        # 'ctd_salinity_munkholmen': {'sea_water_salinity': 'salinity', 'approved': 'approved'}
        #    "sensor_depth":"ctd_depth_munkholmen",
        #    "sensor_position": "meteo_position_munkholmen",
        #    "wind_speed":"meteo_wind_speed_munkholmen",
        #    "wind_direction":"meteo_wind_direction_munkholmen",
        #    "air_temperature":"meteo_temperature_munkholmen",
    }},
    {
        "name": "other", "geometry": {"polygon": [[11,58],[11,59],
                                                  [12,59],[12,58],
                                                  [11,58]]}, "tables": {
        'ctd_temperature_munkholmen': {'sea_water_temperature': 'temperature', 'approved': 'approved'},
        # 'ctd_salinity_munkholmen': {'sea_water_salinity': 'salinity', 'approved': 'approved'}
        # "sensor_depth": "ctd_depth_munkholmen",
        # "sensor_position": "meteo_position_munkholmen",
        # "wind_speed": "meteo_wind_speed_munkholmen",
        # "wind_direction": "meteo_wind_direction_munkholmen",
        # "air_temperature": "meteo_temperature_munkholmen",
    }}
]
parameters_def = {"sea_water_temperature":{
                      "id":"sea_water_temperature",
                       "type": "Quantity",
                       "cj_type": "PointSeries",
                       "name": "sea_water_temperature",
                       "data_type": "float",
                       "description": "Sea water temperature is the in situ temperature of the sea water",
                       "description_lang": "en",
                       "unit_label": "degree_Celsius",
                       "unit_label_lang": "en",
                       "unit_symbol": "Cel",
                       "unit_type": "http://www.opengis.net/def/uom/UCUM/",
                       "observed_property_id": "http://vocab.nerc.ac.uk/standard_name/sea_water_temperature/",
                       "observed_property_label": "Sea Water temperature",
                       "observed_property_label_lang": "en",
                       "unit_type": "float",
                       "min_value": 0,
                       "max_value": 100,
                       "properties": {
                           "madeBySensor": "SensorURI?"
                           # set_property(feature, "resultTime", str(resultTime))
                           #     set_property(feature, "phenomenonTime", str(phenomenonTime))
                           # set_property(feature, "madeBySensor", madeBySensor)
                           #     set_property(feature, "hasFeatureOfInterest", hasFeatureOfInterest)
                           # set_property(feature, "observedProperty", observedProperty)
                           #     set_property(feature, "wasOriginatedBy", wasOriginatedBy)
                           # set_property(feature, "deployedSystem", deployedSystem)

                       }
                  },
                  "approved":
                      {"id": "measurement_approved",
                       "label": "providers approved",
                       "label_lang": "en",
                       "type":"Category",
                       "cj_type": "PointSeries",
                       "data_type": "category",
                       "description": "'yes' if passed a data quality filter, 'no' if not, 'none' if no filter applied",
                       "description_lang": "en",
                       "unit_label": "degree_Celsius",
                       "unit_label_lang": "en",
                       "unit_symbol": "Cel",
                       "unit_type": "http://www.opengis.net/def/uom/UCUM/",
                       "observed_property_id": "http://id3iliad.example.com/observedProperties/approved",
                       "unit_type": "integer",
                       "min_value": 0,
                       "max_value": 1,
                       "observed_property_label":"measurement_approved",
                       "observed_property_label_lang": "en",
                       "observed_property_description": "Categorical property determining if the the measurement passed a quality assessment",
                       "observed_property_description_lang": "en",
                       "observed_property_categories": [{
                                     "id": "http://id3iliad.example.com/observedProperties/approved/yes",
                                     "label": "yes",
                                     "label_lang": "en"
                                 },
                                     {
                                         "id": "http://id3iliad.example.com/observedProperties/approved/no",
                                         "label": "no",
                                         "label_lang": "en"
                                     }
                                 ],
                       "properties": {
                           "seeAlso": "https://docs.influxdata.com/influxdb/v1.3/concepts/key_concepts/"
                       },
                       "categoryEncoding": {
                           "http://id3iliad.example.com/observedProperties/approved/yes": 1,
                           "http://id3iliad.example.com/observedProperties/approved/no": 0,
                           "no": 0, "yes": 1

                       },
                       "categoriesMapping": {
                           "yes": 1,
                           "no": 0
                       }
                       },
                  #       "sea_water_salinity":
                  # {"type":"PointSeries",
                  #  "name":"sea_water_salinity",
                  #  "data_type": "float",
                  #  "description": "Sea water salinity is the in situ salinity of the sea water",
                  #  "description_lang": "en",
                  #  "unit_label":"psu",
                  #  "unit_label_lang": "en",
                  #  "unit_symbol": "psu",
                  #  "unit_type": "???",
                  #  "observed_property_id":"http://vocab.nerc.ac.uk/standard_name/sea_water_salinity/"
                  #
                  # }
                  }


def build_covj_property_category(parameter):
    categories = []
    for cat in parameter['observed_property_categories']:
        categories.append(
            Category(
                id=cat['id'],
                label={cat['label_lang']: cat['label']}
                # preferredColor = "green"
            )
        )
    return Parameter(
        id=parameter['id'],
        label={parameter['label_lang']: parameter['label']},
        description={parameter['description_lang']: parameter['description']},
        observedProperty=ObservedProperty(
            id=parameter['observed_property_id'],
            label={parameter['observed_property_label_lang']: parameter['observed_property_label']},
            description={parameter['observed_property_description_lang']: parameter['observed_property_description']},
            categories=categories
        ),
        properties=parameter['properties'],
        categoryEncoding=parameter['categoryEncoding']
    )

print(build_covj_property_category(parameters_def['approved']))


def build_coverage_rangetype():
    rangetype = {
        'type': 'DataRecord',
        'field': []
    }
    for l in locations:
        for table_name in l['tables'].keys():
            variables = l['tables'][table_name]
            for p_name in variables.keys():
                try:
                    parameter = parameters_def[pname]
                except:
                    continue
                try:
                    next(field for field in rangetype['field'] if field["id"] == parameter['name'])
                    continue
                except:
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


print(build_coverage_rangetype());


def build_covj_property_continuous(parameter):
    return Parameter(
                    id=parameter['id'],
                    description={parameter['description_lang']: parameter['description']},
                    unit=Unit(
                        label={parameter['unit_label_lang']: parameter['unit_label']},
                        symbol=Symbol(
                            value=parameter['unit_symbol'],
                            type=parameter['unit_type'])
                    ),
                    observedProperty=ObservedProperty(
                        id=parameter['observed_property_id'],
                        label={parameter['observed_property_label_lang']: parameter['observed_property_label']}
                    ),
                    properties=parameter['properties']
                )

def build_range_continuous(values, parameter):
    return NdArray(axisNames=["x", "y", "t"], shape=[1, 1, len(values)],
                   values=values)


def build_range_category(values, parameter):
    print(parameter)
    return NdArray(axisNames=["x", "y", "t"], shape=[1, 1, len(values)],
                   values=list(map(lambda v: parameter['categoryEncoding'][v], values)))


# bucket, data_table, '2023-05-01T02:25:00Z', '2023-05-01T02:30:00Z'
def build_coverages_collection_covj(bucket, start_time, stop_time):
    _parameters = {}
    _coverages = []
    for l in locations:
        for table_name in l['tables'].keys():
            variables = l['tables'][table_name]
            for p_name in variables.keys():
                column = variables[p_name]
                print(table_name)
                if parameters_def[p_name]['type'] == 'Quantity':
                    _parameters[p_name] =\
                        build_covj_property_continuous(parameters_def[p_name])
                elif parameters_def[p_name]['type'] == 'Category':
                    _parameters[p_name] = \
                        (build_covj_property_category(parameters_def[p_name]))
            query = define_query(bucket, data_table, start_time, stop_time)
            data = query_to_df(url, token, query)
            print(data)
            _ranges = {}
            for p_name in variables.keys():
                column = variables[p_name]
                measurement_values = data[column]
                if parameters_def[p_name]['type'] == 'Quantity':
                    _ranges[p_name] = build_range_continuous(measurement_values, parameters_def[p_name])
                elif parameters_def[p_name]['type'] == 'Category':
                    _ranges[p_name] = build_range_category(measurement_values, parameters_def[p_name])
            print("x:" + str([l['geometry']['point']['y']]))
            print("y:" + str(l['geometry']['point']['y']))
            coverage = Coverage(
                domain=Domain(
                    domainType=DomainType.point_series,
                    referencing=[
                        ReferenceSystemConnectionObject(
                            coordinates=["x", "y"],
                            system=ReferenceSystem(type="GeographicCRS",
                                                   id="http://www.opengis.net/def/crs/OGC/1.3/CRS84")
                        ),
                        ReferenceSystemConnectionObject(
                            coordinates=["t"],
                            system=ReferenceSystem(type="TemporalRS",
                                                   calendar="Gregorian")
                        )
                    ],
                    axes=Axes(
                        x=ValuesAxis[float](values=[l['geometry']['point']['x']]),
                        y=ValuesAxis[float](values=[l['geometry']['point']['y']]),
                        t=ValuesAxis[AwareDatetime](values=data['_time'])
                    )
                ),
                ranges=_ranges
            )
            _coverages.append(coverage)
    cov = CoverageCollection(
        domainType=DomainType.point_series,
        coverages=_coverages,
        parameters=_parameters,
        referencing=[
            ReferenceSystemConnectionObject(
                coordinates=["x", "y"],
                system=ReferenceSystem(type="GeographicCRS",
                                       id="http://www.opengis.net/def/crs/OGC/1.3/CRS84")
            ),
            ReferenceSystemConnectionObject(
                coordinates=["t"],
                system=ReferenceSystem(type="TemporalRS",
                                       calendar="Gregorian")
            )
        ]
    )
    return cov


def build_single_covj():
    cov = Coverage(
        parameters={"ctd_temperature_munkholmen":
            Parameter(
                id="ctd_temperature_munkholmen",
                description={"en": "The potential temperature, in degrees celcius, of the sea water"},
                unit=Unit(
                    label={"en": "degree_Celsius"},
                    symbol=Symbol(
                        value="Cel",
                        type="http://www.opengis.net/def/uom/UCUM/")
                ),
                observedProperty=ObservedProperty(
                    id="http://vocab.nerc.ac.uk/standard_name/sea_water_potential_temperature/",
                    label={"en": "Sea Water Potential Temperature"}
                ),
                properties={
                    "madeBySensor": "sensor definition as URI, alternatively as an object, or string description"}
            ),
            "approved": build_covj_property_category()
        },
        domain=Domain(
            domainType=DomainType.point_series,
            referencing=[
                ReferenceSystemConnectionObject(
                    coordinates=["x", "y"],
                    system=ReferenceSystem(type="GeographicCRS",
                                           id="http://www.opengis.net/def/crs/OGC/1.3/CRS84")
                ),
                ReferenceSystemConnectionObject(
                    coordinates=["t"],
                    system=ReferenceSystem(type="TemporalRS",
                                           calendar="Gregorian")
                )
            ],
            axes=Axes(
                x=ValuesAxis[float](values=x_values),
                y=ValuesAxis[float](values=y_values),
                t=ValuesAxis[AwareDatetime](values=t_values)
            )
        ),
        ranges={
            "ctd_temperature_munkholmen": NdArray(axisNames=["x", "y", "t"], shape=[1, 1, len(m_values)],
                                                  values=m_values),
            "approved": NdArray(axisNames=["x", "y", "t"], shape=[1, 1, len(a_values)],
                                values=list(map(lambda v: approved_map[v], a_values))),
        }
    )
    return cov

def filter_locations(locations, shape):
    filtered_locations = []
    for l in locations:
        g = l['geometry']
        for gk in g.keys():
            if gk == 'point':
                p = Point(g[gk])
            elif gk == 'polygon':
                p = Polygon(g[gk])
            if not shape.disjoint(p):
                filtered_locations.append(l)
    return filtered_locations

# print(filter_locations(locations,[[10.5,58],[10.5,59],[11.5,59],[11.5,58],[10.5,58]]))

# cov = build_single_covj()
cov = build_coverages_collection_covj(bucket, '2023-05-01T02:25:00Z', '2023-05-01T02:30:00Z', )
# print(cov.model_dump_json(exclude_none=True, indent=4))

with open("/Users/piotr/Temp/sintef_sst_sample.covjson", "w") as outfile:
    outfile.write(cov.model_dump_json(exclude_none=True, indent=4))
