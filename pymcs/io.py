from io import StringIO
from pathlib import Path

import pandas as pd


def read_list_to_df(data):
    missing = data[1].split()[1:]
    cols = data[2].split()
    buffer = StringIO(''.join(data[3:]))
    df = pd.read_csv(buffer, sep='\s+', names=cols, na_values=missing)
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


class DBManager:
    dbroot = Path('/cabeus/data/mcs/level2')
    l2_product_string = 'post2d_v*'

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
        return pd.read_parquet(path)

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

    def _read_4h_block(self, item, timestring=None):
        """Read all data for a 4h block into a pd.DataFrame.

        Parameters
        ----------
        timestring : str
            Format yymmddhh
        item: {'header, 'rms', 'nadir', 'limb', 'profiles}
            String selecting the data item of the L2 data file to be read.

        Returns
        -------
        pd.DataFrame
        """
        if timestring is None:
            timestring = self.timestring
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
        return df

    def read_4h_header(self, timestring=None):
        return self._read_4h_block('header', timestring)

    def read_4h_rms(self, timestring=None):
        return self._read_4h_block('rms', timestring)

    def read_4h_nadir(self, timestring=None):
        return self._read_4h_block('nadir', timestring)

    def read_4h_limb(self, timestring=None):
        return self._read_4h_block('limb', timestring)

    def read_4h_profiles(self, timestring=None):
        return self._read_4h_block('profiles', timestring)

    def get_day(self, item, daystring):
        monthfolder = self.dbroot / daystring[:4]
        folderlist = list(monthfolder.glob(f'{daystring}*'))
        bucket = []
        for folder in folderlist:
            bucket.append(self._read_4h_block())




def read_header_data(month):
    """Read the header data into a pd.DataFrame

    Parameters
    ----------
    month: str
        String depicting the month of observations in the format {mmyy}

    Returns
    -------
    pd.DataFrame
    """
    db = DBManager(month)
    return db.header_data
