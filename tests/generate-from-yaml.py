from flask import Blueprint, jsonify
from flask import Flask

from flask_swagger_generator.generators import Generator
from flask_swagger_generator.specifiers import SwaggerVersion
from flask_swagger_generator.utils import SecurityType

swagger_destination_path = '/Users/piotr/repos/pzaborowski/pygeoapi/pygeoapi/tests/PZB-iliad-dto-test-bblocks-1.0.1-swagger.yaml'

# Create the bluepints
blueprint = Blueprint('objects', __name__)

# Create the flask app
app = Flask(__name__)

# Create swagger version 3.0 generator
generator = Generator.of(SwaggerVersion.VERSION_THREE)

# Add security, response and request body definitions
@generator.security(SecurityType.BEARER_AUTH)
@generator.response(status_code=200, schema={'id': 10, 'name': 'test_object'})
@generator.request_body({'id': 10, 'name': 'test_object'})
@blueprint.route('/objects/<int:object_id>', methods=['PUT'])
def update_object(object_id):
    return jsonify({'id': 1, 'name': 'test_object_name'}), 201

app.register_blueprint(blueprint)
generator.generate_swagger(app, destination_path=swagger_destination_path)