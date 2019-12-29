import math
from datetime import date, time
from pathlib import Path

import pandas as pd
from tqdm import tqdm_notebook as tqdm


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
    return time(h,m,int(s), int(ms)).isoformat()


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
    df['datetime'] = pd.to_datetime(date.astype(str) + ' ' + time)
    df.set_index('datetime', inplace=True)


def get_list_of_hourfolders(daystring):
    root = Path('/cabeus/data/mcs/level2')
    dayfolder = root / daystring[:4]
    return list(dayfolder.glob(f'{daystring}*'))


def get_hour_subfiles(subfolder='080301000000'):
    base = Path('/cabeus/data/mcs/level2')
    product = 'post2d_v*'
    folder = base / subfolder[:4] / subfolder
    try:
        folder = list(folder.glob(product))[0]
    except IndexError:
        return None
    filelist = sorted(list(folder.glob('*.out')))
    return filelist


def convert_4hfiles_to_df(subfolder, write=False):
    filelist = get_hour_subfiles(subfolder)
    if filelist is None:
        return pd.DataFrame()
    bucket = []
    for f in filelist:
        l2 = L2Reader(f)
        bucket.append(l2.header.to_frame().T)
    df = pd.concat(bucket)
    if write:
        df.to_parquet(subfolder.parent / f'{subfolder}.parquet')
    return df


def cols_to_numeric(df):
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')


def convert_dayfiles_to_df(daystring, write=False):
    hourfolders = get_list_of_hourfolders(daystring)
    bucket = []
    for folder in tqdm(hourfolders):
        bucket.append(convert_4hfiles_to_df(folder.name))
    df = pd.concat(bucket)
    cols_to_numeric(df)
    if write:
        df.to_parquet(folder.parent / f'{daystring}.parquet')
    return df


def convert_month_to_df(month, write=True):
    root = Path('/cabeus/data/mcs/level2')
    base = root / month
    folders = [item for item in base.glob('*') if item.is_dir()]
    savename = folders[0].parent / f'{month}.parquet'
    if savename.exists():
        return pd.read_parquet(savename)
    bucket = []
    for folder in tqdm(folders):
        bucket.append(convert_4hfiles_to_df(folder.name))
    df = pd.concat(bucket)
    cols_to_numeric(df)
    if write:
        df.to_parquet(folder.parent / f'{month}_header.parquet')
    return len(df)


def run_month_conversion_parallel():
    from dask.distributed import Client
    import dask

    client = Client()
    base = Path('/cabeus/data/mcs/level2')
    months = [p.name for p in list(base.glob('*'))]
    lazy_results = []
    for month in months:
        lazy_result = dask.delayed(convert_month_to_df)(month)
        lazy_results.append(lazy_result)
    # calculations start here:
    dask.compute(*lazy_results)
