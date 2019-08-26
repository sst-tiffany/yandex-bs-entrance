import json
import time as t
import pandas as pd
import numpy as np

import datetime as dt

import warnings
warnings.filterwarnings('ignore')

def calculate_age(born):
    today = dt.datetime.utcfromtimestamp(t.time()).date()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

def p50(series):
    return np.percentile(series, .5)

def p75(series):
    return np.percentile(series, .75)

def p99(series):
    return np.percentile(series, .99)


class TestGetResult:
    def __init__(self, src_data):
        self._src_data = pd.DataFrame(src_data['citizens'])

    def get_birthdays_res(self):
        res = self._src_data.copy()
        res['month'] = res['birth_date'].apply(lambda x: dt.datetime.strptime(x, '%d.%m.%Y').month)
        res['presents'] = res['relatives'].apply(lambda x: len(set(x)))

        res_json = {}
        for month in range(1, 13):
            res_json[str(month)] = json.loads(
                res[(res['month'] == month) & (res['presents'] > 0)][['citizen_id', 'presents']].to_json(orient='records')
            )
        return {'data': res_json}

    def get_age(self):
        res = self._src_data.copy()
        res['age'] = res['birth_date'].apply(lambda x: dt.datetime.strptime(x, '%d.%m.%Y')).apply(calculate_age)
        res = res.groupby('town').agg({'age': [p50, p75, p99]}).reset_index()
        res.columns = ['town', 'p50', 'p75', 'p99']
        return {'data': json.loads(res.to_json(orient='records'))}