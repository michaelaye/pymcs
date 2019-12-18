from pathlib import Path

import cx_Oracle
import pandas as pd


class MCSDB:
    inifile = Path.home() / ".mcs_db.ini"
    example_sql = """select temperature from mcs_data_2d
    where obsdate = 20070101 and obstime = 14486.727
    """

    def __init__(self):
        with self.inifile.open() as f:
            self.url = f.read().strip()
        self.con = cx_Oracle.connect(self.url)

    def test_call(self):
        return self.query(self.example_sql)

    def get_columns(self):
        sql = """select * from mcs_data_2d
        where obsdate = 20070101 and obstime = 14486.727
        """
        return self.query(sql).columns

    def query(self, sql):
        "most basic method, using pure SQL, returning pandas DataFrame."
        return pd.read_sql(sql, self.con)

    def sqlize(self, dic):
        "create SQL condition part from dictionary"
        s = ""
        for k, v in dic.items():
            s += f"{k}={v} and "
        # cut off last 'and '
        return s[:-5]

    def get_cols_by_date(self, cols, datestr):
        sql = f"select {','.join(cols)} from mcs_data_2d where "
        sql += f"obsdate = {datestr}"
        print("Sending this request:")
        print(sql)
        return self.query(sql)
