from covjson_pydantic.coverage import Coverage, CoverageCollection
from covjson_pydantic.domain import Domain, Axes, ValuesAxis, DomainType
from covjson_pydantic.ndarray import NdArray
from covjson_pydantic.parameter import Parameter
from covjson_pydantic.unit import Unit, Symbol
from covjson_pydantic.observed_property import ObservedProperty, Category
from covjson_pydantic.reference_system import ReferenceSystem, ReferenceSystemConnectionObject

import netCDF4 as nc
import datetime


def _var_attr_2_parameter():
    None


# Replace 'your_trajectories.nc' with the path to your CF trajectories NetCDF file
netcdf_file_path = '/Users/piotr/tmp/simulation.nc'
# Open the NetCDF file
nc_file = nc.Dataset(netcdf_file_path, 'r')

# Extract data from NetCDF file
trajectory_ids = nc_file.variables['trajectory'][:]
time_values = nc_file.variables['time'][:]
latitude_values = nc_file.variables['lat'][:]
longitude_values = nc_file.variables['lon'][:]
altitude_values = nc_file.variables['altitude'][:]

# Close the NetCDF file
nc_file.close()

# Create a dictionary to store coverage collection data
coverage_collection_data = {
    'type': 'CoverageCollection',
    'domain': Domain(
        type='Domain',
        domainType='MultiPoint',
        axes={
            't': {'values': [str(time_val) for time_val in time_values]},
            'x': {'values': longitude_values.flatten().tolist(), 'unit': 'degrees_east'},
            'y': {'values': latitude_values.flatten().tolist(), 'unit': 'degrees_north'},
            'z': {'values': altitude_values.flatten().tolist(), 'unit': 'meters'},
        },
    ),
    'parameters': {},  # Dictionary to store parameter information
    'coverages': [],
}

# Populate parameters information
# Assuming 'temperature' and 'humidity' are your variables; add others as needed
parameter_names = ['temperature', 'humidity']
for param_name in parameter_names:
    coverage_collection_data['parameters'][param_name] = Parameter(
        type='Parameter',
        description=param_name.capitalize(),
        unit='units',  # Replace with the actual unit
    )

# Populate coverages
for i, time_val in enumerate(time_values):
    coverage_data = {
        'type': 'Coverage',
        'domain': Domain(
            type='Domain',
            domainType='MultiPoint',
            axes={
                't': {'values': [str(time_val)]},
                'x': {'values': longitude_values.flatten().tolist(), 'unit': 'degrees_east'},
                'y': {'values': latitude_values.flatten().tolist(), 'unit': 'degrees_north'},
                'z': {'values': altitude_values.flatten().tolist(), 'unit': 'meters'},
            },
        ),
        'parameters': {},
        'ranges': {},
    }

    # Populate parameter values for each coverage
    for param_name in parameter_names:
        parameter_values = nc_file.variables[param_name][:,
                           i]  # Assuming parameter data is a 2D array in the NetCDF file
        coverage_data['parameters'][param_name] = Parameter(
            type='Parameter',
            description=param_name.capitalize(),
            unit='units',  # Replace with the actual unit
            values=parameter_values.tolist(),
        )

    coverage_collection_data['coverages'].append(coverage_data)

# Create a CoverageJSON object using CovJSON Pydantic models
coverage_collection = CovJSON(**coverage_collection_data)

# Print or save the CoverageJSON
print(coverage_collection.json(indent=2))

# If you want to save the CoverageJSON to a file
with open('output_coverage_collection.json', 'w') as output_file:
    output_file.write(coverage_collection.json(indent=2))
