import logging

from flask import Blueprint, request, jsonify
from .service import Service, RelationsError, PatchCitizenError, SelectError, ImportIdNotFound
from .schema import Import as ImportSchema, CitizenPatch as CitizenPatchSchema


logger = logging.getLogger(__name__)

endpoint = Blueprint('imports', __name__, url_prefix='/imports')

import_schema = ImportSchema()
citizen_schema = CitizenPatchSchema()
service = Service()


def _jsonify(func):
    def _wrapper(resp):
        res = func(resp)
        return jsonify(res)

    return _wrapper


@_jsonify
def handle_err(err):
    logger.warning(f'Error type={repr(type(err))} message={err}')
    if isinstance(err, Exception):
        return {'err_type': repr(type(err)), 'message': str(err)}
    elif isinstance(err, str):
        return {'message': err}
    elif isinstance(err, dict):
        return err


@_jsonify
def handle_success(res):
    logger.info('Request is successfully handled')
    return {"data": res}


@endpoint.route('', methods=['POST'])
def imports():
    data, err = import_schema.load(request.json)
    if err:
        return handle_err(err), 400
    try:
        resp = service.put_citizens(data)
    except RelationsError as err:
        return handle_err(err), 400
    else:
        return handle_success(resp), 201


@endpoint.route('<int:import_id>/citizens', methods=['GET'])
def citizen_collection(import_id):
    try:
        resp = service.get_citizens(import_id)
    except (SelectError, ImportIdNotFound) as err:
        return handle_err(err), 400
    else:
        return handle_success(resp), 200


@endpoint.route('<int:import_id>/citizens/<int:citizen_id>', methods=['PATCH'])
def citizen(import_id, citizen_id):
    data, err = citizen_schema.load(request.json)
    if err:
        return handle_err(err), 400
    try:
        resp = service.patch_citizen(import_id, citizen_id, data)
    except (PatchCitizenError, SelectError) as err:
        return handle_err(err), 400
    else:
        return handle_success(resp), 200


@endpoint.route('<int:import_id>/citizens/birthdays', methods=['GET'])
def birthdays(import_id):
    try:
        resp = service.get_birthdays(import_id)
    except (SelectError, ImportIdNotFound) as err:
        return handle_err(err), 400
    else:
        return handle_success(resp), 200


@endpoint.route('<int:import_id>/towns/stat/percentile/age', methods=['GET'])
def age(import_id):
    try:
        resp = service.get_age(import_id)
    except (SelectError, ImportIdNotFound) as err:
        return handle_err(err), 400
    else:
        return handle_success(resp), 200
