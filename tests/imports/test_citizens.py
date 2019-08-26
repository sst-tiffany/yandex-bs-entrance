from copy import deepcopy
from collections import defaultdict
import pytest
from psycopg2.extras import RealDictCursor

from ..utils.post import TestGenPost
from ..utils.patch import TestGenPatch


post_gen = TestGenPost()
DATA = post_gen.generate_valid_test(length=25)
patch_gen = TestGenPatch(src_data=DATA)


@pytest.fixture
def import_id(client):
    rv = client.post('/imports', json=DATA)
    print(rv.json)
    assert rv.status_code == 201
    return rv.json['data']['import_id']


@pytest.mark.parametrize('data, exp_resp', [
    (patch_gen.generate_valid_test(), 200),
    (patch_gen.generate_wrong_relations(mode='float'), 400),
    (patch_gen.generate_wrong_dates(mode='future'), 400),
    (patch_gen.generate_wrong_dates(mode='not_exists'), 400),
    (patch_gen.generate_valid_json_messed(), 200),
    (patch_gen.generate_invalid_data_type_test(field='town', mode='empty'), 400),
    (patch_gen.generate_invalid_data_type_test(field='town', mode='null'), 400),
    (patch_gen.generate_invalid_data_type_test(field='town', mode='wrong_type'), 400),
    (patch_gen.generate_invalid_data_type_test(field='street', mode='empty'), 400),
    (patch_gen.generate_invalid_data_type_test(field='street', mode='null'), 400),
    (patch_gen.generate_invalid_data_type_test(field='street', mode='wrong_type'), 400),
    (patch_gen.generate_invalid_data_type_test(field='building', mode='empty'), 400),
    (patch_gen.generate_invalid_data_type_test(field='building', mode='null'), 400),
    (patch_gen.generate_invalid_data_type_test(field='apartment', mode='null'), 400),
    (patch_gen.generate_invalid_data_type_test(field='apartment', mode='wrong_type'), 400),
    (patch_gen.generate_invalid_data_type_test(field='birth_date', mode='empty'), 400),
    (patch_gen.generate_invalid_data_type_test(field='building', mode='wrong_type'), 400),
    (patch_gen.generate_invalid_data_type_test(field='birth_date', mode='null'), 400),
    (patch_gen.generate_invalid_data_type_test(field='birth_date', mode='wrong_type'), 400),
    (patch_gen.generate_invalid_data_type_test(field='name', mode='empty'), 400),
    (patch_gen.generate_invalid_data_type_test(field='name', mode='null'), 400),
    (patch_gen.generate_invalid_data_type_test(field='name', mode='wrong_type'), 400),
    (patch_gen.generate_invalid_data_type_test(field='gender', mode='empty'), 400),
    (patch_gen.generate_invalid_data_type_test(field='gender', mode='null'), 400),
    (patch_gen.generate_invalid_data_type_test(field='gender', mode='wrong_type'), 400),
    (patch_gen.generate_invalid_data_type_test(field='relatives', mode='empty'), 200),
    (patch_gen.generate_invalid_data_type_test(field='relatives', mode='null'), 400),
    (patch_gen.generate_invalid_data_type_test(field='relatives', mode='wrong_type'), 400),
    (patch_gen.generate_empty_data(), 400),
    (patch_gen.generate_broken_json_extra_field(), 400),
    (patch_gen.generate_broken_json_missing_fields(field='relatives'), 200),
    (patch_gen.generate_longer_test(length=2), 400),
    (patch_gen.generate_unknown_relative(), 400)
])
def test_patch_citizen(client, import_id, data, exp_resp):
    print(data)
    for citizen in DATA['citizens']:
        citizen_id = citizen['citizen_id']
        rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}', json=data)
        assert rv.status_code == exp_resp


def test_import_id_not_exist(client):
    rv = client.patch(f'/imports/1/citizens/1', json={'town': 'test'})
    assert rv.status_code == 400


def test_citizen_id_not_exist(client, import_id):
    citizen_id = max([citizen['citizen_id'] for citizen in DATA['citizens']]) + 1
    rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}', json={'town': 'test'})
    assert rv.status_code == 400


def test_empty_payload(client, import_id):
    citizen_id = max([citizen['citizen_id'] for citizen in DATA['citizens']]) + 1
    rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}')
    assert rv.status_code == 400


def get_relations(conn, import_id, schema='imports'):
    query = f"""
        select * from {schema}.relation
        where 
            import_id = %s
            and is_active is true
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, (import_id,))
        res = cur.fetchall()

    if not res:
        return

    relations = defaultdict(list)

    for relation in res:
        relations[relation['citizen_id']].append(relation['relative'])

    return relations


def test_leave_relation(client, conn, import_id):
    for citizen in DATA['citizens']:
        citizen_id = citizen['citizen_id']
        relations = get_relations(conn, import_id)
        if citizen_id not in relations:
            continue

        rels = relations[citizen_id]
        leave_relation = rels.pop()
        rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}', json={'relatives': rels})

        assert rv.status_code == 200

        relations = get_relations(conn, import_id)

        if not relations:
            break

        rels = relations[citizen_id]
        assert leave_relation not in rels
        rels = relations[leave_relation]
        assert citizen_id not in rels


def test_join_relation(client, conn, import_id):
    for citizen in DATA['citizens']:
        citizen_id = citizen['citizen_id']
        relations = get_relations(conn, import_id)
        print(relations)

        join_relation = None
        for cid, rels in relations.items():
            if citizen_id not in rels and citizen_id != cid:
                join_relation = cid
                break

        if not join_relation:
            continue

        rels = relations[citizen_id]
        rels.append(join_relation)

        rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}', json={'relatives': rels})

        assert rv.status_code == 200

        relations = get_relations(conn, import_id)
        print(relations)

        rels = relations[citizen_id]
        assert join_relation in rels
        rels = relations[join_relation]
        assert citizen_id in rels


def test_join_self_relation(client, conn, import_id):
    citizen = DATA['citizens'][0]
    citizen_id = citizen['citizen_id']
    rels = citizen['relatives']
    rels.append(citizen_id)
    rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}', json={'relatives': rels})

    assert rv.status_code == 200

    relations = get_relations(conn, import_id)
    assert citizen_id in relations[citizen_id]


def test_join_and_leave(client, conn, import_id):
    citizen = None
    for ctz in DATA['citizens']:
        if ctz['relatives']:
            citizen = deepcopy(ctz)
            break

    if not citizen:
        raise Exception('Unexpected Error. Could not find citizen with relatives')

    citizen_id = citizen['citizen_id']
    relatives = citizen['relatives']
    join_relation = None
    for ctz in DATA['citizens']:
        cid = ctz['citizen_id']
        if cid not in relatives and citizen_id != cid:
            join_relation = cid
            break

    if not join_relation:
        raise Exception('Unexpected Error. Could not find citizen to join')

    leave_relation = relatives.pop()
    relatives.append(join_relation)
    rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}', json={'relatives': relatives})

    assert rv.status_code == 200

    relations = get_relations(conn, import_id)

    assert leave_relation not in relations[citizen_id]
    assert citizen_id not in relations[leave_relation]
    assert join_relation in relations[citizen_id]
    assert citizen_id in relations[join_relation]


def test_empty_relative(client, import_id):
    citizen = None
    for ctz in DATA['citizens']:
        if ctz['relatives']:
            citizen = deepcopy(ctz)
            break

    if not citizen:
        raise Exception('Unexpected Error. Could not find citizen with relatives')

    citizen_id = citizen['citizen_id']

    rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}', json={'relatives': []})

    assert rv.status_code == 200

    assert rv.json['data']['relatives'] == []


@pytest.mark.parametrize('field, val', [
    ('town', 'Test'),
    ('street', 'test'),
    ('building', 'test'),
    ('apartment', 123456789),
    ('name', 'test'),
    ('birth_date', '26.12.2000')
])
def test_patch_other_fields(client, import_id, field, val):
    citizen_id = DATA['citizens'][0]['citizen_id']
    rv = client.patch(f'/imports/{import_id}/citizens/{citizen_id}', json={field: val})
    assert rv.status_code == 200
    assert rv.json['data'][field] == val
