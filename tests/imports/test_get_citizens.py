from ..utils.post import TestGenPost


def test_get_citizens(client):
    gen = TestGenPost()
    data = gen.generate_valid_test(length=1000)

    rv = client.post('/imports', json=data)
    import_id = rv.json['data']['import_id']

    rv = client.get(f'/imports/{import_id}/citizens')

    assert rv.status_code == 200

    for citizen in data['citizens']:
        citizen['relatives'].sort()

    for citizen in rv.json['data']:
        citizen['relatives'].sort()

    assert sorted(rv.json['data'], key=lambda x: x['citizen_id']) == \
           sorted(data['citizens'], key=lambda x: x['citizen_id'])


def test_import_id_not_exist(client):
    rv = client.get(f'/imports/1/citizens')
    assert rv.status_code == 400

