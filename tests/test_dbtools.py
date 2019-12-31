from pymcs import db_tools
import pytest
from datetime import datetime as dt


@pytest.fixture
def mcsdate():
    return (20070101, 14486.727)


def test_dateconverter_init():
    with pytest.raises(ValueError):
        # this should raise:
        _ = db_tools.DATECONVERTER()


def test_dateconverter_datetime(mcsdate):
    datecon = db_tools.DATECONVERTER(mcsdate=mcsdate)
    assert datecon.datetime == dt(2007, 1, 1, 4, 1, 26, 727000)


def test_dateconverter_utcdate(mcsdate):
    datecon = db_tools.DATECONVERTER(mcsdate=mcsdate)
    assert datecon.utcdate == "2007-01-01T04:01:26.727000"


def test_dateconverter_obsdate(mcsdate):
    datecon = db_tools.DATECONVERTER(mcsdate=mcsdate)
    assert datecon.obsdate == 20070101


def test_dateconverter_obstime(mcsdate):
    datecon = db_tools.DATECONVERTER(mcsdate=mcsdate)
    assert pytest.approx(datecon.obstime) == 14486.727


def test_dateconverter_mcsdate(mcsdate):
    datecon = db_tools.DATECONVERTER(mcsdate=mcsdate)
    out_mcsdate = datecon.mcsdate
    assert out_mcsdate[0] == 20070101
    assert pytest.approx(out_mcsdate[1]) == 14486.727

