import pytest

from ..utils.post import TestGenPost


post_gen = TestGenPost()


@pytest.mark.parametrize('data, exp_resp', [
    (post_gen.generate_valid_test(length=10), 201),
    (post_gen.generate_broken_int(length=10, field='citizen_id'), 400),
    (post_gen.generate_broken_int(length=10, field='apartment'), 400),
    (post_gen.generate_broken_json_extra_field(length=10), 400),
    (post_gen.generate_broken_json_missing_field(length=10, field='citizen_id'), 400),
    (post_gen.generate_duplicated_citizen_id(length=10), 400),
    (post_gen.generate_empty_data(), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='citizen_id', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='citizen_id', mode='wrong_type'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='town', mode='empty'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='town', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='town', mode='wrong_type'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='street', mode='empty'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='street', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='street', mode='wrong_type'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='building', mode='empty'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='building', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='building', mode='wrong_type'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='apartment', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='apartment', mode='wrong_type'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='birth_date', mode='empty'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='birth_date', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='birth_date', mode='wrong_type'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='name', mode='empty'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='name', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='name', mode='wrong_type'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='gender', mode='empty'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='gender', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='gender', mode='wrong_type'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='relatives', mode='empty'), 201),
    (post_gen.generate_invalid_data_type_test(length=10, field='relatives', mode='null'), 400),
    (post_gen.generate_invalid_data_type_test(length=10, field='relatives', mode='wrong_type'), 400),
    (post_gen.generate_one_sided_relations(length=10), 400),
    (post_gen.generate_valid_json_messed(length=10), 201),
    (post_gen.generate_wrong_dates(length=10, mode='future'), 400),
    (post_gen.generate_wrong_dates(length=10, mode='not_exists'), 400),
    (post_gen.generate_wrong_relations(length=100, mode='out_of_sample'), 400),
    (post_gen.generate_wrong_relations(length=100, mode='float'), 400),
    (post_gen.generate_wrong_relations(length=10, mode='str'), 400),
    (post_gen.generate_wrong_relations(length=100, mode='negative'), 400),
    (post_gen.generate_wrong_relations(length=100, mode='repetitive'), 400),
])
def test_imports(client, data, exp_resp):
    print(data)
    rv = client.post('/imports', json=data)
    print(rv.json)
    assert rv.status_code == exp_resp


def test_empty_payload(client):
    rv = client.post(f'/imports')
    print(rv.json)
    assert rv.status_code == 400
