from ..utils.post import TestGenPost
from ..utils.get_result import TestGetResult


def test_get_birthdays(client):
    gen = TestGenPost()
    data = gen.generate_valid_test(length=1000)

    rv = client.post('/imports', json=data)
    import_id = rv.json['data']['import_id']

    rv = client.get(f'/imports/{import_id}/citizens/birthdays')

    assert rv.status_code == 200

    result_gen = TestGetResult(src_data=data)
    res_data = result_gen.get_birthdays_res()

    for month, value in res_data['data'].items():
        res_data['data'][month] = value.sort(key=lambda x: x['citizen_id'])

    for month, value in rv.json['data'].items():
        rv.json['data'][month] = value.sort(key=lambda x: x['citizen_id'])

    assert res_data == rv.json


def test_import_id_not_exist(client):
    rv = client.get(f'/imports/1/citizens/birthdays')
    assert rv.status_code == 400
