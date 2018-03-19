
__author__ = 'Mohamed Amine Guessous' # Mohamed Amine Guessous

#
# Copyright 2016 Cuemacro
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
Write and reads time series data to disk through Arctic library
"""

from pycaishen.ioengines.ioengine import IOEngine
from pycaishen.util.loggermanager import LoggerManager
from pycaishen.util.settings import IO_Arctic_configuration as IO_Settings

from arctic import Arctic
from datetime import datetime as dt
import pandas as pd
import pymongo

old = False

class IOEngineArctic(IOEngine):



    def __init__(self):

        super(IOEngine, self).__init__()
        if IO_Settings.IO_VERBOSE_MODE:
            self.logger = LoggerManager().getLogger(__name__)
        return

    def _connect_original(self):

        # setting the connection to the store
        host = IO_Settings.HOST

        socketTimeoutMS = 10 * 1000
        c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)
        # store = Arctic(host)

        if IO_Settings.IO_VERBOSE_MODE:
            self.logger.info("Connect to the mongo-host / clustera" + host)



        return store,c

    def _connect(self):

        # setting the connection to the store
        host = IO_Settings.HOST

        # socketTimeoutMS = 10 * 1000
        # c = pymongo.MongoClient(host, connect=False)
        # Connect to the mongo-host / cluster
        # store = Arctic(c, socketTimeoutMS=socketTimeoutMS, serverSelectionTimeoutMS=socketTimeoutMS)
        store = Arctic(host)

        if IO_Settings.IO_VERBOSE_MODE:
            self.logger.info("Connect to the mongo-host / clustera" + host)

        return store

    def _check_lib(self,store,library_name):
        # check in the library exist if not initialize one
        try:
            database = store[library_name]
        except:
            # returns None if the library do not exist
            trials = 5
            i = 0
            #TODO try to find better solution to the failed library initialisation
            while (i <= trials):
                i = i + 1
                try:
                    store.initialize_library(library_name)
                    if IO_Settings.IO_VERBOSE_MODE:
                        self.logger.info("Created MongoDB library: " + library_name)
                    break
                except:
                    if IO_Settings.IO_VERBOSE_MODE:
                        self.logger.info("Failed to Created MongoDB library: " + library_name)
                        self.logger.info("trial nbr: %d " % i)


    def write(self, symbol, data_frame,library_name , append_data = False, metadata = None ):
        """

        :param symbol: name of the data
        :param data_frame: data to be saved
        :param host: equivalent to DB for SQL, path for CSV, Excel and HD5, store in Arctic
        :param library: equivalent to table, library in Arctic
        :param append_data:
        :return:
        """
        #
        if data_frame.empty:
            self.logger.warn("Empty dataframe... !! " )
            return

        # Connect to the store
        if old:
            (store,c) = self._connect_original()
        else:
            store = self._connect()

        # check if the library exist if not create
        self._check_lib(store,library_name)


        library = store[library_name]

        # can duplicate values if we have existing dates
        if metadata is None:
            if append_data:
                #TODO check why it is not working properly
                library.append(symbol, data_frame)
            else:
                library.write(symbol, data_frame)
            if IO_Settings.IO_VERBOSE_MODE:
                self.logger.info("Written MongoDB library: " + symbol)
        elif isinstance(metadata,dict):
            if append_data:
                library.append(symbol, data_frame,metadata=metadata)
            else:
                library.write(symbol, data_frame, metadata= metadata)
            if IO_Settings.IO_VERBOSE_MODE:
                self.logger.info("Written MongoDB library: " + symbol)

        else:
            if IO_Settings.IO_VERBOSE_MODE:
                self.logger.info("metadata format not correct, check if it's a dictionary " )

        if old :
            c.close()

        return

    def read(self, symbol , library_name , start_date = None, finish_date = None,  metadata = None ):
        """

        :param symbol: name of the data
        :param library: equivalent to table, library in Arctic
        :param host: equivalent to DB for SQL, path for CSV, Excel and HD5, store in Arctic
        :return: dataframe of the data requested
        """
        # Connect to the store
        if old:
            (store,c) = self._connect_original()
        else:
            store = self._connect()

        # check if the library exist if not create
        self._check_lib(store, library_name)

        # Access the library
        library = store[library_name]
        try :

            if metadata == None:
                if start_date is None and finish_date is None:
                    item = library.read(symbol)
                else:
                    from arctic.date import DateRange
                    item = library.read(symbol, date_range=DateRange(start_date, finish_date))
            else :
                if start_date is None and finish_date is None:
                    item = library.read(symbol,metadata=metadata)
                else:
                    from arctic.date import DateRange
                    item = library.read(symbol, date_range=DateRange(start_date, finish_date),metadata=metadata)

            if IO_Settings.IO_VERBOSE_MODE:
                self.logger.info('Read ' + symbol)
            if old :
                c.close()

            return item.data
        except:
            self.logger.warn("No data available for %s ",symbol)
            return pd.DataFrame()

    def delete_symbol(self,symbol , library_name  ):
        """

        :param symbol:
        :param library:

        :param host:
        :return:
        """

        # Connect to the store
        if old:
            (store,c) = self._connect_original()
        else:
            store = self._connect()

        # check if the library exist if not create
        self._check_lib(store, library_name)

        # Access the library
        library = store[library_name]

        item = library.delete(symbol)
        if IO_Settings.IO_VERBOSE_MODE:
            self.logger.info('delete ' + symbol)

        if old :
            c.close()

        return

    def delete_library(self, library_name):
        """

        :param symbol:
        :param library:

        :param host:
        :return:
        """

        # Connect to the store
        if old:
            (store,c) = self._connect_original()
        else:
            store = self._connect()

        try:
            store.delete_library(library_name)
        except:
            if IO_Settings.IO_VERBOSE_MODE:
                self.logger.info("library not available")
            return
        if IO_Settings.IO_VERBOSE_MODE:
            self.logger.info('delete ' + library_name)

        if old:
            c.close()

        return

    def list_symbols(self, library_name, key = None ):
        """

        :param library:
        :param key:
        :param host:
        :return:
        """

        # Connect to the store
        if old:
            (store,c) = self._connect_original()
        else:
            store = self._connect()

        # check if the library exist if not create
        self._check_lib(store, library_name)


        # Access the library
        library = store[library_name]

        # What symbols (keys) are stored in the library
        if key is None:
            symbols = library.list_symbols()
            if old:
                c.close()
            return symbols
        else :
            if isinstance(key,dict):
                symbols = library.list_symbols(metadata = key)
                if old:
                    c.close()
                return symbols
            else:
                return


    def list_libraries(self):
        """

        :param host:
        :return:
        """

        # Connect to the store
        if old:
            (store,c) = self._connect_original()
        else:
            store = self._connect()

        list_lib= store.list_libraries()
        if old:
            c.close()
        return list_lib

    def has_symbol(self, symbol, library_name):

        """

        :param symbol:
        :param library:
        :param host:
        :return:
        """

        # Connect to the store
        if old:
            (store,c) = self._connect_original()
        else:
            store = self._connect()

        # check if the library exist if not create
        self._check_lib(store, library_name)

        # Access the library
        library = store[library_name]

        exists = library.has_symbol(symbol)
        if IO_Settings.IO_VERBOSE_MODE:
            self.logger.info('Check if ' + symbol + ' exists')
        if old:
            c.close()

        return exists


if __name__ == '__main__':

    import pandas as pd
    libs = "azertyuiop"
    libs = list(libs)
    libs = ["test." + x for x in libs]
    print(libs)

    df = pd.DataFrame(libs,columns=["lol"])

    storage = IOEngineArctic()
    symbol  ="baba ghanouch"
    for lib in libs:
        storage.write(symbol,df,lib)
    for lib in libs:
        print((storage.read(symbol,lib)))
    for lib in libs:
        print((storage.list_symbols(lib)))
    for lib in libs:
        print((storage.has_symbol(symbol,lib)))

    for lib in libs:
        storage.delete_library(lib)

    print((storage.list_libraries()))

    def arctic_stocks():
        import ystockquote
        import collections
        import pandas

        def get_stock_history(ticker, start_date, end_date):
            data = ystockquote.get_historical_prices(ticker, start_date, end_date)
            df = pandas.DataFrame(collections.OrderedDict(sorted(data.items()))).T
            df = df.convert_objects(convert_numeric=True)
            return df




        engine = IOEngineArctic()
        host ='127.0.0.1'
        library = 'sto'
        symbol = 'aapl'
        key1 ={'source':'Yahoo'}

        print(("getting data for " + symbol))
        df = get_stock_history(symbol, '2015-01-01', '2015-02-01')

        print("print writing data to the DB")
        engine.write(symbol, df, library,metadata=key1)

        print("reading data from the DB")
        print((engine.read(symbol,library)))

        print("getting new data")
        df2 = get_stock_history(symbol, '2015-02-01', '2015-03-01')

        print("wrinting the new data into the database ")
        engine.write(symbol, df2, library,append_data=True)

        print("Reading the full data")
        print((engine.read(symbol, library)))

        symbol = 'c'
        key1 = {'source': 'Yahoo'}

        print(("getting data for " + symbol))
        df = get_stock_history(symbol, '2015-01-01', '2016-02-01')

        print("print writing data to the DB")
        engine.write(symbol, df, library, metadata=key1)

        print("Reading the full data")
        print((engine.read(symbol, library)))

        # print engine.list_symbols(library,key2)
        print("list libraries")
        print((engine.list_libraries()))
        print("listing available symbols")
        print((engine.list_symbols(library)))


        print("testing delete")
        print(("deleting " + symbol + " from library"))
        engine.delete_symbol(symbol,library)

        print("checking the results ")
        print((engine.list_symbols(library)))

        print("test has-symbol")
        print((engine.has_symbol(symbol,library)))

        symbol = 'aapl'
        print((engine.has_symbol(symbol, library)))

        print("test delete library ")
        engine.delete_library(library)

        print("remain libraries")
        print((engine.list_libraries()))

    arctic_stocks()