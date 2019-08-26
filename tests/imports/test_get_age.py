from ..utils.post import TestGenPost
from ..utils.get_result import TestGetResult


def test_get_age(client):
    gen = TestGenPost()
    data = gen.generate_valid_test(length=1000)

    rv = client.post('/imports', json=data)
    import_id = rv.json['data']['import_id']

    rv = client.get(f'/imports/{import_id}/towns/stat/percentile/age')

    assert rv.status_code == 200

    result_gen = TestGetResult(src_data=data)
    res_data = result_gen.get_age()

    res_data['data'] = res_data['data'].sort(key=lambda x: x['town'])
    rv.json['data'] = rv.json['data'].sort(key=lambda x: x['town'])

    assert res_data == rv.json


def test_import_id_not_exist(client):
    rv = client.get(f'/imports/1/towns/stat/percentile/age')
    assert rv.status_code == 400
