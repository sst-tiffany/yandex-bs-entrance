import pandas as pd
from numpy import NaN

from .dbm import DBManager


class RelationsError(Exception):
    ...


class InsertError(Exception):
    ...


class PatchCitizenError(Exception):
    ...


class ImportIdNotFound(Exception):
    ...


class SelectError(Exception):
    ...


class MockService:
    def put_citizens(self, data):
        ...


class Service:
    def __init__(self):
        self._dbm = DBManager()

    def put_citizens(self, data):
        citizens = data['citizens']

        # relations check
        check_df = self._check_relatives_data_prep(citizens)
        self._check_relatives_present(check_df)
        self._check_relatives_two_sided(check_df)

        # good quality data insert
        try:
            import_id = self._dbm.insert_citizens(citizens)
        except Exception as err:  # TODO: Exception more detailed
            raise InsertError(err)

        return import_id

    def _check_relatives_data_prep(self, citizens):
        check_data = {
            'citizen': [],
            'relative': []
        }
        for citizen_info in citizens:
            citizen_id = citizen_info['citizen_id']
            relatives = citizen_info['relatives']

            if relatives:
                check_data['citizen'] += [citizen_id] * len(relatives)
                check_data['relative'] += relatives
            else:
                check_data['citizen'] += [citizen_id]
                check_data['relative'] += [NaN]

        check_df = pd.DataFrame(check_data)
        # print('citizens data collected')
        return check_df

    def _check_relatives_present(self, check_df):
        if set(check_df[check_df['relative'].notnull()]['relative']) - set(check_df['citizen']):
            raise RelationsError('relative not found in citizen list')
        # print('check1 passed')

    def _check_relatives_two_sided(self, check_df):
        check_df = pd.merge(
            check_df,
            check_df.rename(columns={'citizen': 'relative', 'relative': 'relatives_relative'}),
            how='outer',
            on=['relative']
        )
        check_df = check_df[check_df['relative'].notnull()]
        check_df['found'] = check_df['citizen'] == check_df['relatives_relative']
        check_df = check_df.groupby(['citizen', 'relative'])['found'].sum().reset_index()

        if len(check_df) != sum(check_df['found']):
            raise RelationsError('some relations are not two-sided')

    def patch_citizen(self, import_id, citizen_id, citizen_upd):
        citizen_ids = self._patch_citizen_check_data_prep(import_id)
        self._patch_citizen_check_citizen_exists(citizen_id, citizen_ids)

        relatives = self._patch_citizen_relatives_get(citizen_upd)
        self._patch_citizen_check_relatives_exist(relatives, citizen_ids)

        citizen_info = self._patch_citizen_get_citizen_info(import_id, citizen_id)
        ex_relatives, new_relatives = self._patch_citizen_analyze_citizen_changes(citizen_info, relatives)

        self._patch_citizen_update_citizen(import_id, citizen_info, citizen_upd)

        if ex_relatives:
            for ex in ex_relatives:
                self._patch_citizen_update_relation(import_id, citizen_id, ex, 'remove')

        if new_relatives:
            for new in new_relatives:
                self._patch_citizen_update_relation(import_id, citizen_id, new, 'add')

        citizen_info_updated = self._dbm.get_citizen(import_id, citizen_id)
        return citizen_info_updated

    def _patch_citizen_check_data_prep(self, import_id):
        try:
            citizen_ids = self._dbm.get_citizen_ids(import_id)
        except Exception as err:  # TODO: Exception more detailed
            raise SelectError(err)

        if not citizen_ids['citizen_ids']:
            raise PatchCitizenError(f'No such import_id={import_id}')

        citizen_ids = set(citizen_ids['citizen_ids'])
        return citizen_ids

    def _patch_citizen_check_citizen_exists(self, citizen_id, citizen_ids):
        if citizen_id not in citizen_ids:
            raise PatchCitizenError(f'No such citizen_id={citizen_id}')

    def _patch_citizen_relatives_get(self, data):
        relatives = None
        if 'relatives' in data:
            relatives = data['relatives']
        return relatives

    def _patch_citizen_check_relatives_exist(self, relatives, citizen_ids):
        if relatives != None and set(relatives) - citizen_ids:
            raise PatchCitizenError('some relative not found')

    def _patch_citizen_get_citizen_info(self, import_id, citizen_id):
        citizen_info = self._dbm.get_citizen(import_id, citizen_id)
        return citizen_info

    def _patch_citizen_analyze_citizen_changes(self, old_citizen_info, relatives):
        ex_relatives, new_relatives = None, None
        if relatives != None:
            old_relatives = old_citizen_info['relatives']
            ex_relatives = set(old_relatives) - set(relatives)
            new_relatives = set(relatives) - set(old_relatives)

        return ex_relatives, new_relatives

    def _patch_citizen_update_citizen(self, import_id, citizen_info, citizen_upd):
        citizen_info.update(citizen_upd)
        citizen_info.update({'import_id': import_id})
        self._dbm.update_citizen(citizen_info)

    def _patch_citizen_update_relation(self, import_id, citizen_id, relative_id, mode='add'):
        for left, right in zip([citizen_id, relative_id], [relative_id, citizen_id]):
            relation = {
                'import_id': import_id,
                'citizen_id': left,
                'relative': right,
                'is_active': mode == 'add'
            }
            self._dbm.update_relation(relation)

    # endpoint 3: get citizens
    def get_citizens(self, import_id):
        import_ids = self._dbm.get_import_ids()
        if import_id not in import_ids:
            raise ImportIdNotFound(f'No such import_id={import_id}')
        try:
            citizens = self._dbm.get_citizens(import_id)
        except Exception as err:  # TODO: Exception more detailed
            raise SelectError(err)

        return citizens

    # endpoint 4: get birthdays
    def get_birthdays(self, import_id):
        import_ids = self._dbm.get_import_ids()
        if import_id not in import_ids:
            raise ImportIdNotFound(f'No such import_id={import_id}')
        try:
            birthdays = self._dbm.get_birthdays(import_id)
        except Exception as err:  # TODO: Exception more detailed
            raise SelectError(err)

        return birthdays

    # endpoint 5: get age
    def get_age(self, import_id):
        import_ids = self._dbm.get_import_ids()
        if import_id not in import_ids:
            raise ImportIdNotFound(f'No such import_id={import_id}')
        try:
            age = self._dbm.get_age(import_id)
        except Exception as err:  # TODO: Exception more detailed
            raise SelectError(err)

        return age
