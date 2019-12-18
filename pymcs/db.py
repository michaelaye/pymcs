from pathlib import Path

import cx_Oracle
import pandas as pd


class MCSDB:
    inifile = Path.home() / ".mcs_db.ini"
    base_search = "select temperature from mcs_data_2d where "
    example_dict = {"obsdate": 20070101, "obstime": 14486.727}

    def __init__(self):
        with self.inifile.open() as f:
            self.url = f.read().strip()
        self.con = cx_Oracle.connect(self.url)

    def test_call(self):
        return self.get_data_by_dict(self.example_dict)

    def send_sql(self, sql):
        "most basic method, using pure SQL, returning pandas DataFrame."
        return pd.read_sql(sql, self.con)

    def sqlize(self, dic):
        "create SQL condition part from dictionary"
        s = ""
        for k, v in dic.items():
            s += f"{k}={v} and "
        # cut off last 'and '
        return s[:-5]

    def get_condition(self, cond):
        "write your own condition string"
        return self.send_sql(self.base_search + cond)

    def get_data_by_dict(self, dict):
        return self.get_condition(self.sqlize(dict))

    def get_cols_by_date(self, cols, datestr):
        sql = f"select {','.join(cols)} from mcs_data_2d where "
        sql += f"obsdate = {datestr}"
        print("Sending this request:")
        print(sql)
        return self.send_sql(sql)
