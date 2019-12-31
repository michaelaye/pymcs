from datetime import datetime as dt
from pathlib import Path
import math

import cx_Oracle
import pandas as pd

mars_years = {
    28: "2006-01-21T20:35:00",
    29: "2007-12-09T20:06:00",
    30: "2009-10-26T19:39:00",
    31: "2011-09-13T19:12:00",
    32: "2013-08-01T18:44:00",
    33: "2015-06-19T18:16:00",
    34: "2017-05-05T17:48:00",
    35: "2019-03-24T17:20:00",
    36: "2021-01-15T16:52:00",
}


def mcsdate2datetime(mcsdate):
    "Convert (OBSDATE,OBSTIME) tuple to Python datetime."
    date, seconds = mcsdate
    date = str(date)
    yyyy = int(date[:4])
    mm = int(date[4:6])
    dd = int(date[6:8])
    fractionals, intseconds = math.modf(seconds)
    hours = int(intseconds // 3600)
    minutes = int((intseconds % 3600) // 60)
    seconds = int(intseconds % 3600 % 60)
    microsecs = int(fractionals * 1e6)
    return dt(yyyy, mm, dd, hours, minutes, seconds, microsecs)


class DATECONVERTER:
    """Manage UTC ISO datetime to MCS date conversions.

        MCS has stored its data in the form of OBSDATE/OBSTIME, with OBSDATE
        being an integer in the form YYYYMMDD and OBSTIME in total seconds of
        the date (i.e. 0...(3600*24=86,400)).

        Parameters
        ----------
        utcdate : str,datetime
            UTC datetime
        mcsdate : tuple(int, float)
            Tuple of (OBSDATE, OBSTIME)

        Attributes
        ----------
        utcdate : str
            Return datetime.isoformat()
        mcsdate : tuple
            Return datetime converted to MCS OBSDATE,OBSTIME
        """

    OBSDATE_FMT = "%Y%M%d"

    def __init__(self, utcdate=None, mcsdate=None):
        if not any([utcdate, mcsdate]):
            raise ValueError("One of [utcdate, mcsdate] needs to be defined.")
        if utcdate is not None:
            self.datetime = dt.fromisoformat(utcdate)
            self._utcdate = utcdate
        self._utcdate = utcdate
        if mcsdate is not None:
            self.datetime = mcsdate2datetime(mcsdate)
            self._mcsdate = mcsdate

    @property
    def utcdate(self):
        return self.datetime.isoformat()

    @property
    def obstime(self):
        dt = self.datetime
        return dt.hour * 3600 + dt.minute * 60 + dt.second + dt.microsecond / 1e6

    @property
    def obsdate(self):
        return int(self.datetime.strftime(self.OBSDATE_FMT))

    @property
    def mcsdate(self):
        return (self.obsdate, self.obstime)


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
    d = {"profile": views[1], "header": views[0], "limb": views[2], "nadir": views[3]}

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
            cols = ",".join(cols)
        sql = f"""
            select {cols}
            from {self.d['profile']}
            where {cond}
        """
        return self.query(sql)
