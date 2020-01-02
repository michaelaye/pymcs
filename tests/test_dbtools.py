from datetime import datetime as dt

import pandas as pd
import pytest

from pymcs import db_tools


@pytest.fixture
def mcsdate():
    return (20070101, 14486.727)


def test_dateconverter_init():
    with pytest.raises(ValueError):
        # this should raise:
        _ = db_tools.DateConverter()


def test_dateconverter_datetime(mcsdate):
    datecon = db_tools.DateConverter(mcsdate=mcsdate)
    assert datecon.datetime == dt(2007, 1, 1, 4, 1, 26, 727000)


def test_dateconverter_utcdate(mcsdate):
    datecon = db_tools.DateConverter(mcsdate=mcsdate)
    assert datecon.utcdate == "2007-01-01T04:01:26.727000"


def test_dateconverter_obsdate(mcsdate):
    datecon = db_tools.DateConverter(mcsdate=mcsdate)
    assert datecon.obsdate == 20070101


def test_dateconverter_obstime(mcsdate):
    datecon = db_tools.DateConverter(mcsdate=mcsdate)
    assert pytest.approx(datecon.obstime) == 14486.727


def test_dateconverter_mcsdate(mcsdate):
    datecon = db_tools.DateConverter(mcsdate=mcsdate)
    out_mcsdate = datecon.mcsdate
    assert out_mcsdate[0] == 20070101
    assert pytest.approx(out_mcsdate[1]) == 14486.727


def test_get_MY_bracket():
    t1, t2 = db_tools.get_MY_bracket(31)
    assert t1 == "2011-09-13T19:12:00"
    assert t2 == "2013-08-01T18:44:00"


def test_DateConverter_set_datetime():
    t1, t2 = db_tools.get_MY_bracket(31)
    datecon = db_tools.DateConverter(utcdate=t1)
    datecon.datetime = dt.fromisoformat(t2)
    assert datecon.utcdate == "2013-08-01T18:44:00"


def test_SQLizer():
    sqlizer = db_tools.SQLizer(["obsdate", "obstime"], "profile", "obsdate = 20070101")
    sqlizer.start_sql()
    assert (
        sqlizer.sql
        == "select obsdate,obstime from MCS_PROFILE_DATA where obsdate = 20070101"
    )


def test_add_utc_col():
    from io import StringIO

    txt = """
TNADIR	OBSTIME	OBSDATE	LS
0	4	12776.435	20120617	125.92
1	4	12807.154	20120617	125.92
2	4	12837.874	20120617	125.92
3	4	12868.594	20120617	125.92
4	4	12899.313	20120617	125.92
"""
    df = pd.read_csv(StringIO(txt), sep=r"\s+")
    newdf = db_tools.add_utc_col(df)
    assert hasattr(newdf, "UTC")
    assert newdf.UTC.iloc[0].isoformat() == "2012-06-17T03:32:56.434999"
