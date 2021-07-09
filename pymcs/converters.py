# AUTOGENERATED! DO NOT EDIT! File to edit: notebooks/01_converters.ipynb (unless otherwise specified).

__all__ = ['get_time', 'get_date', 'add_datetime_column', 'get_list_of_hourfolders', 'get_hour_subfiles',
           'convert_4hfiles_to_df', 'cols_to_numeric', 'convert_dayfiles_to_df', 'convert_month_to_df',
           'run_month_conversion_parallel']

# Cell
import math
from datetime import date, time
from pathlib import Path

import pandas as pd
from tqdm import tqdm_notebook as tqdm

# Cell
def get_time(seconds):
    """Convert MCS time to datetime.time.

    Parameters
    ----------
    seconds : float
        Seconds of the day passed (max: 86400)

    Returns
    -------
    datetime.time
    """
    h = int(seconds / 3600)
    rest = seconds % 3600
    m = int(rest / 60)
    s = rest % 60
    ms, s = math.modf(s)
    ms = round(ms, 6) * 1_000_000
    return time(h, m, int(s), int(ms)).isoformat()

# Cell
def get_date(dateint):
    """Convert MCS date integer into datetime.date

    Parameters
    ----------
    dateint : int
        Integer for the date in format yyyymmdd

    Returns
    -------
    datetime.date
    """
    datestr = str(dateint)
    return date(int(datestr[:4]), int(datestr[4:6]), int(datestr[6:8]))

# Cell
def add_datetime_column(df):
    """Add full datetime column to MCS L2 dataframe.

    Using get_time and get_date helpers, combine them into a full datetime
    column and add it to the incoming dataframe.
    Set index to the new datetime column.

    Parameters
    ----------
    df : pd.DataFrame
        A MCS dataframe that has a `Date` and a `Time` column in the formats
        yyyymmdd (int) for the date and seconds of the day for time, respectively.

    Returns
    -------
    Nothing, new column is added to the incoming dataframe and then made into
    the index.
    """
    time = df.Time.map(get_time)
    date = df.Date.map(get_date)
    df["datetime"] = pd.to_datetime(date.astype(str) + " " + time)
    df.set_index("datetime", inplace=True)

# Cell
def get_list_of_hourfolders(daystring):
    "For a given day(string), find all hour folders."
    root = Path("/cabeus/data/mcs/level2")
    dayfolder = root / daystring[:4]
    return list(dayfolder.glob(f"{daystring}*"))

# Cell
def get_hour_subfiles(subfolder="080301000000"):
    "Get sorted list of .out files for given subfolder."
    base = Path("/cabeus/data/mcs/level2")
    product = "post2d_v*"
    folder = base / subfolder[:4] / subfolder
    try:
        folder = list(folder.glob(product))[0]
    except IndexError:
        return None
    filelist = sorted(list(folder.glob("*.out")))
    return filelist

# Cell
def convert_4hfiles_to_df(subfolder, write=False):
    "Read in the .out files and convert to pandas DataFrame for given subfolder."
    filelist = get_hour_subfiles(subfolder)
    if filelist is None:
        return pd.DataFrame()
    bucket = []
    for f in filelist:
        l2 = L2Reader(f)
        bucket.append(l2.header.to_frame().T)
    df = pd.concat(bucket)
    if write:
        df.to_parquet(subfolder.parent / f"{subfolder}.parquet")
    return df

# Cell
def cols_to_numeric(df):
    "Change each column's dtype to a numerical one, if it's still a str/object."
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="ignore")

# Cell
def convert_dayfiles_to_df(daystring, write=False):
    "For a given day(string), convert all data to a pandas DataFrame and store as parquet."
    hourfolders = get_list_of_hourfolders(daystring)
    bucket = []
    for folder in tqdm(hourfolders):
        bucket.append(convert_4hfiles_to_df(folder.name))
    df = pd.concat(bucket)
    cols_to_numeric(df)
    if write:
        df.to_parquet(folder.parent / f"{daystring}.parquet")
    return df

# Cell
def convert_month_to_df(month, write=True):
    "Convert all data for a given month into a dataframe and store as parquet."
    root = Path("/cabeus/data/mcs/level2")
    base = root / month
    folders = [item for item in base.glob("*") if item.is_dir()]
    savename = folders[0].parent / f"{month}.parquet"
    if savename.exists():
        return pd.read_parquet(savename)
    bucket = []
    for folder in tqdm(folders):
        bucket.append(convert_4hfiles_to_df(folder.name))
    df = pd.concat(bucket)
    cols_to_numeric(df)
    if write:
        df.to_parquet(folder.parent / f"{month}_header.parquet")
    return len(df)

# Cell
def run_month_conversion_parallel():
    "Run the month conversion in parallelized form using dask.delayed."
    import dask
    from dask.distributed import Client

    client = Client()
    base = Path("/cabeus/data/mcs/level2")
    months = [p.name for p in list(base.glob("*"))]
    lazy_results = []
    for month in months:
        lazy_result = dask.delayed(convert_month_to_df)(month)
        lazy_results.append(lazy_result)
    # calculations start here:
    dask.compute(*lazy_results)