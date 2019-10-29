from io import StringIO
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from .converters import add_datetime_column


def read_list_to_df(data):
    missing = data[1].split()[1:]
    cols = data[2].split()
    buffer = StringIO(''.join(data[3:]))
    df = pd.read_csv(buffer, sep=r'\s+', names=cols, na_values=missing)
    return df


class L2Reader:
    def __init__(self, fname):
        with open(fname, 'r') as f:
            self.data = f.readlines()
        self.parse_data()

    def parse_data(self):
        comments_found = 0
        self.header_dic = {}
        self.rms_data = []
        self.limb_data = []
        self.nadir_data = []
        self.profiles_data = []

        for line in self.data:
            if line.startswith(' ### '):
                comments_found += 1
                continue
            if comments_found < 1:
                continue
            if comments_found == 1:
                key, value = line.split('=')
                self.header_dic[key.strip()] = value.strip()
            if comments_found == 2:
                self.rms_data.append(line)
            if comments_found == 3:
                self.limb_data.append(line)
            if comments_found == 4:
                self.nadir_data.append(line)
            if comments_found == 5:
                self.profiles_data.append(line)

    @property
    def header(self):
        return pd.Series(self.header_dic)

    @property
    def rms(self):
        return read_list_to_df(self.rms_data)

    @property
    def limb(self):
        return read_list_to_df(self.limb_data)

    @property
    def nadir(self):
        return read_list_to_df(self.nadir_data)

    @property
    def profiles(self):
        return read_list_to_df(self.profiles_data)


def cols_to_numeric(df):
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')


class DBManager:
    dbroot = Path('/cabeus/data/mcs/level2')
    l2_product_string = 'post2d_v*'
    items = ['header', 'rms', 'limb', 'nadir', 'profiles']

    def __init__(self, month=None, timestring=''):
        self.month = month
        self._timestring = timestring

    @property
    def timestring(self):
        return self._timestring.ljust(12, '0')

    @timestring.setter
    def timestring(self, value):
        self._timestring = value

    @property
    def header_data(self):
        path = self.dbroot / f"{self.month}/{self.month}_header.parquet"
        df = pd.read_parquet(path)
        add_datetime_column(df)
        return df

    def get_hour_subfiles(self, timestring=None):
        """Get list of files inside 4h block folder.

        Parameters
        ----------
        timestring: str
            Format: yymmddhh
        """
        if timestring is not None:
            self.timestring = timestring
        folder = self.dbroot / self.timestring[:4] / self.timestring
        try:
            productfolder = list(folder.glob(self.l2_product_string))[0]
        except IndexError:
            return []
        filelist = sorted(list(productfolder.glob('*.out')))
        return filelist

    def _get_4h_block(self, item, timestring_or_path):
        """Read all data for a 4h block into a pd.DataFrame.

        Parameters
        ----------
        timestring_or_path : str, pathlib.Path
            String of format yymmddhh[0000] or Path to 4h block directory
        item: {'header, 'rms', 'nadir', 'limb', 'profiles'}
            String selecting the data item of the L2 data file to be read.

        Returns
        -------
        pd.DataFrame
        """
        try:
            timestring = timestring_or_path.name
        except AttributeError:
            timestring = timestring_or_path

        self.last_timestring = timestring
        filelist = self.get_hour_subfiles(timestring)
        if not filelist:
            raise FileNotFoundError(f'No files found for {timestring}.')
        bucket = []
        for f in filelist:
            l2 = L2Reader(f)
            data = getattr(l2, item)
            if item == 'header':
                bucket.append(data.to_frame().T)
            else:
                bucket.append(data)
        df = pd.concat(bucket, ignore_index=True)
        cols_to_numeric(df)
        return df

    def get_4h_header(self, timestring):
        return self._get_4h_block('header', timestring)

    def get_4h_rms(self, timestring):
        return self._get_4h_block('rms', timestring)

    def get_4h_nadir(self, timestring):
        return self._get_4h_block('nadir', timestring)

    def get_4h_limb(self, timestring):
        return self._get_4h_block('limb', timestring)

    def get_4h_profiles(self, timestring):
        return self._get_4h_block('profiles', timestring)

    def get_available_4h_folders(self, daystring):
        "Find available files for a day."
        monthfolder = self.dbroot / daystring[:4]
        return list(monthfolder.glob(f'{daystring}*'))

    def get_day(self, item, daystring):
        """Get MCS data for a calendar day.

        Parameters
        ----------
        item : {'header, 'rms', 'nadir', 'limb', 'profiles'}
            String selecting the data item of the L2 data file to be read.
        daystring : str
            Day format string in format yymmdd

        Returns
        -------
        pd.DataFrame
            DataFrame with combined data from all MCS data for the given day.
        """
        folderlist = self.get_available_4h_folders(daystring)
        bucket = []
        for folder in tqdm(folderlist):
            bucket.append(self._get_4h_block(item, folder.name))
        return pd.concat(bucket, ignore_index=True)

    def get_day_header(self, daystring):
        return self.get_day('header', daystring)

    def get_day_rms(self, daystring):
        return self.get_day('rms', daystring)

    def get_day_limb(self, daystring):
        return self.get_day('limb', daystring)

    def get_day_nadir(self, daystring):
        return self.get_day('nadir', daystring)

    def get_day_profiles(self, daystring):
        return self.get_day('profiles', daystring)


def get_header_data(month):
    """Read the header data into a pd.DataFrame

    Returns pd.DataFrame with all data for the given month combined, all
    data columns of the correct type and a datetime column as index (derived from
    Date and Time columns) for automatic plotting over time.

    Parameters
    ----------
    month: str
        String selecting the month of observations in the format {mmyy}

    Returns
    -------
    pd.DataFrame
       DataFrame with all header data for the given month combined (approx 45 MB)
       and the index set to a full functional datetime object, for improved plotting
       over time.
    """
    db = DBManager(month)
    return db.header_data
