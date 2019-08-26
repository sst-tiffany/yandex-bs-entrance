import json
import time as t
import pandas as pd
import numpy as np

import datetime as dt
from string import ascii_letters, digits

import warnings

warnings.filterwarnings('ignore')


class TestGenPost:
    def __init__(self, seed=13):
        self._case_name = 'default'
        self._today_unixtime = int(t.time())
        self._default_symbols = list(ascii_letters + digits)

        self._types = {
            'int': 'int',
            'text': 'text',
            'date': 'text',
            'gender': 'text',
            'list': 'list'
        }
        self._field_types = {
            'citizen_id': 'int',
            'town': 'text',
            'street': 'text',
            'building': 'text',
            'apartment': 'int',
            'name': 'text',
            'birth_date': 'date',
            'gender': 'gender',
            'relatives': 'list'
        }
        self._gen_funcs = {
            'int': None,
            'text': None,
            'date': None,
            'gender': None,
            'list': None
        }
        self._empty_types = {
            'int': None,
            'text': '',
            'date': '',
            'gender': '',
            'list': []
        }

        self._gen_funcs['text'] = self._gen_valid_text_field
        self._gen_funcs['int'] = self._gen_valid_int_field
        self._gen_funcs['date'] = self._gen_valid_date_field
        self._gen_funcs['gender'] = self._gen_valid_gender
        self._gen_funcs['list'] = self._gen_valid_list

        np.random.seed(seed)

    def generate_valid_test(self, length, path=None):
        self._case_name = 'valid_test'
        self._gen_valid_test(length)
        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_valid_test(self, length):
        self._length = length
        self._word_lengths = [i for i in np.random.randint(1, 50, length)]
        self._unix_timestamps = np.random.randint(0, self._today_unixtime, length)
        self._relative_pairs_cnt = length // 3

        self._get_data_by_setup()
        self._gen_valid_relations()

    def _test_data_to_json(self, path):
        json_data = json.loads(self._data.to_json(orient='records'))
        json_data = {'citizens': json_data}

        if path:
            with open(f'{path}/{self._case_name}_{self._length}.json', 'w') as f:
                json.dump(json_data, f)
            return None
        else:
            return json_data

    def _get_data_by_setup(self):
        data = {}
        for field, f_type in self._field_types.items():
            data[field] = self._gen_funcs[f_type]()

        self._data = pd.DataFrame(data)

    def _gen_valid_int_field(self):
        length = self._length
        return list(np.random.choice(range(length * 3), length, replace=False))

    def _gen_valid_text_field(self):
        symbols = self._default_symbols
        word_lengths = self._word_lengths
        return [''.join(np.random.choice(symbols, word_len)) for word_len in word_lengths]

    def _gen_valid_date_field(self):
        unix_timestamps = self._unix_timestamps
        return [dt.datetime.utcfromtimestamp(unix_ts).date().strftime('%d.%m.%Y') for unix_ts in unix_timestamps]

    def _gen_valid_gender(self):
        length = self._length
        return np.random.choice(['male', 'female'], length)

    def _gen_valid_list(self):
        length = self._length
        return [[i for i in range(np.random.choice(range(3)))] for l in range(length)]

    def _gen_valid_relations(self):
        pairs_cnt = self._relative_pairs_cnt
        data = self._data

        if 'relatives' in data.columns:
            del data['relatives']

        relations = pd.DataFrame(
            [sorted(np.random.choice(data['citizen_id'], 2)) for i in range(pairs_cnt)],
            columns=['citizen_id', 'relatives']
        ).drop_duplicates()

        relations = pd.concat([
            relations,
            relations.rename(columns={'citizen_id': 'relatives', 'relatives': 'citizen_id'})
        ], sort=False)

        relations = relations.groupby('citizen_id').agg({'relatives': lambda x: list(set(x))}).reset_index()

        data = pd.merge(data, relations, how='left', on='citizen_id')
        data['relatives'] = data['relatives'].apply(lambda x: x if isinstance(x, list) else [])

        self._data = data.copy()

    def generate_invalid_data_type_test(self, length, field, mode='wrong_type', path=None):
        self._case_name = f'invalid_test_{field}_{mode}'
        self._gen_valid_test(length)

        self._gen_broken_field(field, mode)

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_broken_field(self, field, mode):
        length = self._length
        if mode == 'empty':
            field_type = self._field_types[field]
            self._data[field] = [self._empty_types[field_type]] * length
        elif mode == 'null':
            self._data[field] = [None] * length
        elif mode == 'wrong_type':
            wrong_type = self._random_wrong_type(field)
            self._data[field] = self._gen_funcs[wrong_type]()

    def _random_wrong_type(self, field):
        field_type = self._field_types[field]
        data_type = self._types[field_type]
        return np.random.choice([d_type for d_type in self._types.values() if d_type != data_type])

    def generate_empty_data(self, path=None):
        self._case_name = f'invalid_test_empty_data'
        json_data = {'citizens': []}

        if path:
            with open(f'{path}/{self._case_name}.json', 'w') as f:
                json.dump(json_data, f)
        else:
            return json_data

    def generate_wrong_dates(self, length, mode='future', path=None):
        self._case_name = f'invalid_test_date_{mode}'
        self._gen_valid_test(length)

        self._gen_broken_dates(mode)

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_broken_dates(self, mode):
        today_unixtime = self._today_unixtime
        unix_timestamps = self._unix_timestamps

        if mode == 'future':
            wrong_dates = [dt.datetime.utcfromtimestamp(unix_ts + today_unixtime).date() for unix_ts in unix_timestamps]
            wrong_dates = [(w_date + dt.timedelta(1)).strftime('%d.%m.%Y') for w_date in wrong_dates]
            self._data['birth_date'] = wrong_dates
        elif mode == 'not_exists':
            self._data['birth_date'] = '30.02.2019'

    def generate_broken_int(self, length, field, mode='negative', path=None):
        self._case_name = f'invalid_test_{field}_{mode}'
        self._gen_valid_test(length)

        self._gen_broken_int(field, mode)

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_broken_int(self, field, mode):
        if mode == 'negative':
            self._data[field] = self._data[field] * -1 - 1
        elif mode == 'float':
            self._data[field] = self._data[field].astype(float)

    def generate_broken_json_missing_field(self, length, field, path=None):
        self._case_name = f'invalid_test_{field}_missing'
        self._gen_valid_test(length)

        self._gen_missing_field(field)

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_missing_field(self, field):
        del self._data[field]

    def generate_broken_json_extra_field(self, length, path=None):
        self._case_name = f'invalid_test_extra_field'
        self._gen_valid_test(length)

        self._gen_extra_field()

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_extra_field(self):
        self._data['extra'] = None

    def generate_valid_json_messed(self, length, path=None):
        self._case_name = f'valid_test_messed'
        self._gen_valid_test(length)

        self._gen_mess()

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_mess(self):
        self._data = self._data[sorted(list(self._data.columns), reverse=True)]

    def generate_duplicated_citizen_id(self, length, path=None):
        self._case_name = f'invalid_test_citizen_id_duplicated'
        self._gen_valid_test(length)

        self._gen_citizen_duplicated()

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_citizen_duplicated(self):
        self._data = pd.concat([self._data for i in range(2)])

    def generate_one_sided_relations(self, length=1000, path=None):
        self._case_name = f'invalid_test_one_sided_relations'
        self._gen_valid_test(length)

        self._gen_one_sided_relations()

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_one_sided_relations(self):
        ind = self._data[self._data['relatives'].apply(lambda x: True if len(x) else False)].index[0]
        self._data['relatives'] = [[el for el in val if i != ind] for i, val in self._data['relatives'].items()]

    def generate_wrong_relations(self, length, mode='out_of_sample', path=None):
        self._case_name = f'invalid_test_relations_{mode}'
        self._gen_valid_test(length)

        self._gen_wrong_relations(mode)

        json_res = self._test_data_to_json(path)
        if json_res:
            return json_res

    def _gen_wrong_relations(self, mode):
        if mode == 'out_of_sample':
            max_citizen_id = self._data['citizen_id'].max()
            self._data['relatives'] = self._data['relatives'].apply(
                lambda x: [max_citizen_id + val for val in x] if len(x) else x
            )
        elif mode == 'float':
            self._data['relatives'] = self._data['relatives'].apply(
                lambda x: [float(val) for val in x] if len(x) else x
            )
        elif mode == 'str':
            self._data['relatives'] = self._data['relatives'].apply(
                lambda x: ['a' for val in x] if len(x) else x
            )
        elif mode == 'negative':
            self._data['relatives'] = self._data['relatives'].apply(
                lambda x: [-val - 1 for val in x] if len(x) else x
            )
        elif mode == 'repetitive':
            self._data['relatives'] = self._data['relatives'].apply(
                lambda x: x * 2 if len(x) else x
            )
