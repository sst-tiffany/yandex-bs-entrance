import pandas as pd
from numpy import NaN

from .dbm import DBManager


class RelationsError(Exception):
    ...


class InsertError(Exception):
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