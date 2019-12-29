from pathlib import Path

import cx_Oracle
import pandas as pd


class MCSDB:
    inifile = Path.home() / ".mcs_db.ini"
    example_sql = """select temperature from mcs_data_2d
    where obsdate = 20070101 and obstime = 14486.727
    """
    views = ["MCS_HEADER_DATA", "MCS_PROFILE_DATA", "MCS_LIMB_DATA", "MCS_NADIR_DATA"]
    tables = [
        "MCS_HEADER_TEST",
        "MCS_PROFILE_TEST",
        "MCS_PROFILE_2D",
        "MCS_HEADER_2D",
        "MCS_NADIR",
        "MCS_HEADER",
        "MCS_PROFILE",
        "MCS_LIMB",
    ]
    d = {'profile': views[1],
         'header': views[0],
         'limb': views[2],
         'nadir': views[3]}

    def __init__(self):
        with self.inifile.open() as f:
            self.url = f.read().strip()
        self.con = cx_Oracle.connect(self.url)

    def test_call(self):
        return self.query(self.example_sql)

    def get_columns(self, table_or_view):
        sql = f"""select * from {table_or_view}
        where obsdate = 20070101 and obstime = 14486.727
        """
        return self.query(sql).columns

    @property
    def header_columns(self):
        return self.get_columns("mcs_header_data")

    @property
    def profile_columns(self):
        return self.get_columns("mcs_profile_data")

    @property
    def limb_columns(self):
        return self.get_columns("mcs_limb_data")

    @property
    def nadir_columns(self):
        return self.get_columns("mcs_nadir_data")

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

    def get_profile_data(self, cols, cond):
        """Get data from the MCS profile view.

        Parameters
        ----------
        cols : str, list
            Either a comma separated string of column names or a Python list
            of columns names, both should work.
        cond : str
            SQL filtering condition, e.g. 'obsdate = 20070101'

        Returns
        -------
        pd.DataFrame
        """
        if isinstance(cols, list):
            cols = ','.join(cols)
        sql = f"""
            select {cols}
            from {self.d['profile']}
            where {cond}
        """
        return self.query(sql)
