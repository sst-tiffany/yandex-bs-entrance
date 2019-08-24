import logging

from flask import Blueprint, request, jsonify
from .service import Service, RelationsError
from .schema import Import as ImportSchema


logger = logging.getLogger(__name__)

endpoint = Blueprint('imports', __name__)

import_schema = ImportSchema()
service = Service()


@endpoint.route('/imports', methods=['POST'])
def imports():
    if request.method == 'POST':
        req_body = request.json
        logger.info(f'req_body={req_body}')
        return imports_post(req_body)


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


def imports_post(req_body):
    data, err = import_schema.load(req_body)
    if err:
        return handle_err(err), 400
    try:
        resp = service.put_citizens(data)
    except RelationsError as err:
        return handle_err(err), 400
    else:
        return handle_success(resp), 201


# def imports_patch(req_body):
#     data, err = schema.load(req_body)
#     if err:
#         return 400, handle_err(err)
#     try:
#         resp = service.update_citizen(data)
#     except RelationsError as err:
#         return 400, handle_err(err)
#     except Exception as err:
#         return 500, INTERNAL_ERROR
#     else:
#         return 201, handle_success(resp)
