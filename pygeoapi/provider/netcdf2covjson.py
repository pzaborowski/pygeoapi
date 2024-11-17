import json
import xarray as xr
import netCDF4
import numpy as np
import argparse
import cftime

from pygeoapi.provider.base import ProviderNoDataError, ProviderQueryError
from pygeoapi.provider.base_edr import BaseEDRProvider
from pygeoapi.provider.xarray_ import (
    _to_datetime_string,
    _convert_float32_to_float64,
    XarrayProvider,
)


def gen_time_series_profile(_data, x, y, t, x_val, y_val):
    """
    timeSeries profile has time and timeSeries or just time as dimensions

    :param data: source data
    """
    out_meta = {
            "driver": "xarray",
            "variables": {var_name: var.attrs
                          for var_name, var in _data.variables.items()}
        }
    xmax = xmin = ymax = ymin = None
    if x:
        if _data.variables.get(x):
            xmax = _data.variables.get(x).max()
            xmin = _data.variables.get(x).min()
    if y:
        if _data.variables.get(y):
            ymax = _data.variables.get(y).max()
            ymin = _data.variables.get(y).min()
    if (xmax!=None) & (xmin!=None) & (ymax!=None) & (ymin!=None):
        out_meta["bbox"] = [xmin, ymin, xmax, ymax]

    out_meta["time"], out_meta["time_steps"] = _parse_time_metadata(_data, t)
    ts = _data.sizes.get('timeSeries')
    if (ts is not None):
        ts_len = len(ts)
    else:
        ts_len = 1
    if ts_len > 1:
        json = {"type": "CoverageCollection",
                "domainType": "PointSeries",
                "coverages": []}
        for ts in _data.coords.get('timeSeries'):
            ts_id = ts.values.item()
            sdata = _data.sel(timeSeries=ts_id)
            json["coverages"].append(
                _gen_one_time_series_profile(
                    sdata, x, y, t))
    else:
        json = _gen_one_time_series_profile(_data, x, y, t, x_val, y_val)


def _gen_one_time_series_profile(c_data, x, y, t, x_val, y_val):
    print(c_data)
    cov = {
        "type": "Coverage",
        "domain" : {
            "type" : "Domain",
            "domainType": "PointSeries",
            "axes": {
                "x": {"values": [x_val]},
                "y": {"values": [y_val]},
                "t": {"values": [date.strftime('%Y-%m-%d %H:%M:%S') for date in c_data[t].values.flatten().tolist()]}
            },
            "referencing": [{
                "coordinates": ["x", "y"],
                "system": {
                    "type": "GeographicCRS",
                    "id": "http://www.opengis.net/def/crs/OGC/1.3/CRS84"
                }
                },
                {
                "coordinates": ["t"],
                "system": {
                    "type": "TemporalRS",
                    "calendar": "Gregorian"
                }
                }
            ]
        },
        'parameters': {},
        'ranges': {}
    }
    fields = get_fields(c_data)
    for key, value in fields.items():
        parameter = {
                'type': 'Parameter',
                'description': {'en': value['title']},
                'unit': {
                    'symbol': value['x-ogc-unit']
                },
                'observedProperty': {
                    'id': key,
                    'label': {
                        'en': value['title']
                    }
                }
            }

        cov['parameters'][key] = parameter
    return cov


variables_map = {"Temperature":{"long_name":"Sea Water temperature","unit":"degC"}}


def get_fields(_data):
    _fields = {}
    for key, value in _data.variables.items():
        if key not in _data.coords:
            dtype = value.dtype
            if dtype.name.startswith('float'):
                dtype = 'float'
            elif dtype.name.startswith('int'):
                dtype = 'integer'
            unit = value.attrs.get('units')
            if not unit:
                unit = variables_map[key]["unit"]
            name = value.attrs.get('long_name')
            if not name:
                name = variables_map[key]["long_name"]
            _fields[key] = {
                    'type': dtype,
                    'title': name,
                    'x-ogc-unit': unit
                }
        return _fields


def _parse_time_metadata(data, time_field):
    """
    Parse time information for output metadata.

    :param data: xarray dataset

    :returns: list of temporal extent, number of timesteps
    """
    try:
        time = _get_time_range(data, time_field)
    except KeyError:
        time = []
    try:
        time_steps = data.coords[time_field].size
    except KeyError:
        time_steps = 1
    return time, time_steps


def _get_time_range(data, time_field):
        """
        Make xarray dataset temporal extent

        :param data: xarray dataset

        :returns: list of temporal extent
        """
        time = data.coords[time_field]
        if time.size == 0:
            raise ProviderNoDataError()
        else:
            start = _to_datetime_string(data[time_field].values.min())
            end = _to_datetime_string(data[time_field].values.max())
        return [start, end]


    


def netcdf_to_coveragejson(data, x, y, t, profile, x_val, y_val):
    # Open the NetCDF file

    if profile == "timeSeries":
        return gen_time_series_profile(data, x, y, t, x_val, y_val)

    # Extract data
    lon = data.variables['lon'][:]
    lat = data.variables['lat'][:]
    time = data.variables['time'][:]
    data = data.variables['data'][:]

    # Create CoverageJSON structure
    coveragejson = {
        "type": "Coverage",
        "domain": {
            "type": "Domain",
            "axes": {
                "x": {"values": lon.tolist()},
                "y": {"values": lat.tolist()},
                "t": {"values": time.tolist()}
                }
            },
        "parameters": {
            "data": {
                "type": "Parameter",
                "description": "Sample data",
                "unit": {"symbol": "unknown"}
                }
            },
        "ranges": {
            "data": {
                "type": "NdArray",
                "dataType": "float",
                "axisNames": ["t", "y", "x"],
                "shape": data.shape,
                "values": data.flatten().tolist()
                }
            }
        }
    return coveragejson


def main():
    parser = argparse.ArgumentParser(description='Convert NetCDF to CoverageJSON.')
    parser.add_argument('netcdf_file', type=str, help='Input NetCDF file')
    parser.add_argument('output_file', type=str, help='Output CoverageJSON file')
    parser.add_argument('x', type=str, help='x variable in the source file')
    parser.add_argument('y', type=str, help='y variable in the source file')
    parser.add_argument('t', type=str, help='t variable in the source file')
    parser.add_argument('sprofile', type=str, help='source file profile: timeSeries|grid')
    args = parser.parse_args()

    data = xr.load_dataset(args.netcdf_file)
    coveragejson = netcdf_to_coveragejson(data,
                                          args.x,
                                          args.y,
                                          args.t,
                                          args.sprofile,
                                          10.12, 30.22)
    # Write to output file
    with open(args.output_file, 'w') as f:
        json.dump(coveragejson, f, indent=2)

    print(f"Converted {args.netcdf_file} to {args.output_file}")

if __name__ == '__main__':
    main()