__author__ = 'Mohamed Amine Guessous' # Mohamed Amine Guessous

#
# Copyright 2016
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and limitations under the License.
#


"""

IOEngine

Abstract class for various data engines.

Write and reads time series data to disk in various formats, CSV and HDF5 format. (planning to add other interfaces too).
Also supports BColz (but not currently stable).

"""

from pycaishen.util.loggermanager import LoggerManager
# from pycaishen.util.dataconstants import DataConstants

from pycaishen.util.settings import IO_configuration as IO_Settings
import abc
import pandas as pd

class IOEngine(object):
    def __init__(self):
        """
        Abstract class for reading and writing data
        """
        if IO_Settings.IO_VERBOSE_MODE :
            self.logger = LoggerManager().getLogger(__name__)

        return

    @abc.abstractmethod
    def write(self, ):
        return

    @abc.abstractmethod
    def read(self,):
        return

    @abc.abstractmethod
    def delete_symbol(self,):
        return

    @abc.abstractmethod
    def delete_library(self, ):
        return

    @abc.abstractmethod
    def list_symbols(self,):
        return

    @abc.abstractmethod
    def list_libraries(self):
        return

    @abc.abstractmethod
    def has_symbol(self):
        return

    def get_tickers_columns(self,dataframe, tickers):
        """
        list of columns that have the tickers name in it
        :param tickers: list of tickers
        :param dataframe: datafrae holding data
        :return: list of columns that have the tickers name in it and it data
        will use it to split the dataframe into specific dataframe for one ticker only
        """
        matching = []
        columns = dataframe.columns
        if type(tickers) is str:
            tickers = [tickers]
        for ticker in tickers:
            match = [s for s in columns if ticker in s]
            matching.append(match)

        return matching

    def split_df_by_tickers(self,dataframe, tickers):
        """
        this function return a list of dataframe for specific organized by tickers
        :param df: dataframe containing data of multiple tickers
        :return: list of dataframe for specific organized by tickers
        """
        columns_header = self.get_tickers_columns(dataframe, tickers)
        df = []  # will get the list of dataframes
        for columns in columns_header:
            df.append(pd.DataFrame(dataframe, columns=columns))

        return df



