import math
from collections import OrderedDict
from datetime import date
from datetime import datetime as dt
from datetime import timedelta
from pathlib import Path

import cx_Oracle
import numpy as np
import pandas as pd

d = {
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

mars_years = OrderedDict(sorted(d.items(), key=lambda t: t[0]))

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

views = ["MCS_HEADER_DATA", "MCS_PROFILE_DATA", "MCS_LIMB_DATA", "MCS_NADIR_DATA"]

view_dic = {
    "profile": views[1],
    "header": views[0],
    "limb": views[2],
    "nadir": views[3],
}


def get_MY_bracket(MY):
    t1 = mars_years[MY]
    t2 = mars_years[MY + 1]
    return (t1, t2)


def get_MY_for_date(date):
    "date: YYYYMMDD"
    for k, v in mars_years.items():
        datestr = v.split("T")[0]
        datestr = datestr.replace("-", "")
        if int(date) < int(datestr):
            return k - 1


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


class MarsTimer:
    MCS_day_format = "%Y%m%d"

    def __init__(self):
        for k, v in mars_years.items():
            setattr(self, f"MY{k}", dt.fromisoformat(v))

    def get_date_for_Ls(self, Ls, MY):
        t = getattr(self, f"MY{MY}")
        delta = timedelta(days=round(self.calc_ndays_for_Ls(Ls, MY)))
        return t + delta

    def get_ndays_for_MY(self, MY):
        date1 = dt.fromisoformat(mars_years[MY])
        date2 = dt.fromisoformat(mars_years[MY + 1])
        delta = (date2 - date1).days
        return delta

    def get_MCS_date_for_Ls(self, Ls, MY):
        dt = self.get_date_for_Ls(Ls, MY)
        return dt.strftime(self.MCS_day_format)

    def calc_ndays_for_Ls(self, Ls, MY):
        fraction = Ls / 360
        n_days = self.get_ndays_for_MY(MY)
        return n_days * fraction

    def get_n_MCS_days_later(self, daystring, n=1):
        delta_days = timedelta(days=n)
        old_date = date(int(daystring[:4]), int(daystring[4:6]), int(daystring[6:]))
        new_date = old_date + delta_days
        return new_date.strftime(self.MCS_day_format)


class DateConverter:
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

    OBSDATE_FMT = "%Y%m%d"

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


def sqlize(self, dic):
    "create SQL condition part from dictionary"
    s = ""
    for k, v in dic.items():
        s += f"{k}={v} and "
    # cut off last 'and '
    return s[:-5]


class SQLizer:
    def __init__(self, columns, view, cond=None):
        self.columns = columns + "obsdate obstime ls".split()
        self.view = view_dic[view]
        self.cond = cond
        self.condition_started = False
        self.bucket = [f"select {','.join(self.columns)} from {self.view}"]
        if self.cond is not None:
            self.bucket.append(f"where {self.cond}")
            self.condition_started = True

    def add_condition(self, condition):
        "Default here is to add conditions with logical AND."
        first_word = "and"
        if not self.condition_started:
            first_word = "where"
            self.condition_started = True
        self.bucket.append(f"{first_word} {condition}")

    def add_day_bracket(self, t1, t2):
        datecon1 = DateConverter(utcdate=t1)
        datecon2 = DateConverter(utcdate=t2)
        self.add_condition(f"obsdate between {datecon1.obsdate} and {datecon2.obsdate}")

    def add_MY_day_bracket(self, MY):
        t1, t2 = get_MY_bracket(MY)
        self.add_day_bracket(t1, t2)

    def add_LS_bracket(self, LS1, LS2):
        self.add_condition(f"LS between {LS1} and {LS2}")

    def add_LAT_bracket(self, lat1, lat2):
        self.add_condition(f"LATITUDE between {lat1} and {lat2}")

    def add_P_bracket(self, p1, p2):
        self.add_condition(f"PRESSURE between {p1} and {p2}")

    def exact_utcdate(self, utcdate):
        "utcdate: YYYYMMDD"
        self.add_condition(f"obsdate = {utcdate}")

    @property
    def sql(self):
        return "\n".join(self.bucket)

    def __str__(self):
        return self.sql

    def __repr__(self):
        return self.__str__()


def add_utc_col(df, drop_mcsdate=True):
    "Must have OBSDATE and OBSTIME column."
    date = pd.to_datetime(df.OBSDATE.astype(str), format="%Y%m%d").astype("str")
    fractionals, intseconds = np.modf(df.OBSTIME)
    hours = (intseconds // 3600).astype("int").astype("str")
    minutes = ((intseconds % 3600) // 60).astype("int").astype("str")
    seconds = (intseconds % 3600 % 60).astype("int").astype("str")
    microsecs = (fractionals * 1e6).astype("int").astype("str")
    newdf = df.assign(
        UTC=pd.to_datetime(
            date + " " + hours + ":" + minutes + ":" + seconds + "." + microsecs
        )
    )
    if drop_mcsdate:
        newdf.drop(["OBSDATE", "OBSTIME"], axis="columns", inplace=True)
    return newdf


class MCSDB:
    inifile = Path.home() / ".mcs_db.ini"
    example_sql = """select temperature from mcs_profile_data
    where obsdate = 20070101 and obstime = 14486.727
    """

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
        try:
            return pd.read_sql(sql.sql, self.con)
        except AttributeError:
            return pd.read_sql(sql, self.con)

    def get_cols_by_date(self, cols, datestr):
        "Deprecated. Use get_profile_cols_by_date."
        sql = f"select {','.join(cols)} from mcs_data_2d where "
        sql += f"obsdate = {datestr}"
        print("Sending this request:")
        print(sql)
        return self.query(sql)

    def get_profile_cols_by_date(self, cols, datestr):
        "Deprecated. Use get_profile_cols_by_date."
        sql = f"select {','.join(cols)} from mcs_profile_data where "
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
