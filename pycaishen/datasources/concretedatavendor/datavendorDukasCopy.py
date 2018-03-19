from pycaishen.datasources.idatasource import IDataSource
from pycaishen.util.loggermanager import LoggerManager



"""
DataVendorDukascopy

Class for downloading tick data from DukasCopy (note: past month of data is not available). Selecting very large
histories is not recommended as you will likely run out memory given the amount of data requested.

Parsing of files is rewritten version https://github.com/nelseric/ticks/
- parsing has been speeded up considerably
- on-the-fly downloading/parsing

"""

import os
from datetime import timedelta
from pycaishen.util.settings import Dukascopy_configuration as Dukas_Settings
import pandas
import requests

try:
    from numba import jit
finally:
    pass

# decompress binary files fetched from Dukascopy
try:
    import lzma
except ImportError:
    try:
        from backports import lzma
    except ImportError:
        import pylzma as lzma


# abstract class on which this is based
from pycaishen.datasources.idatasource import IDataSource

# for logging and constants
from pycaishen.util import  LoggerManager

class DataSourceDukasCopy(IDataSource):
    tick_name  = "{symbol}/{year}/{month}/{day}/{hour}h_ticks.bi5"

    def __init__(self):
        super(IDataSource, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

        import logging
        logging.getLogger("requests").setLevel(logging.WARNING)
    

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        """
        load_ticker - Retrieves pybbg-test data from external data source (in this case Bloomberg)

        Parameters
        ----------
        market_data_request : TimeSeriesRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        DataFrame
        """

        # market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        return self.get_data(market_data_request)

    def kill_session(self):

        return


    def get_data(self,market_data_request):
        data_frame = None
        self.logger.info("Request Dukascopy data")

        # doesn't support non-tick data
        if (market_data_request.freq in ['daily', 'weekly', 'monthly', 'quarterly', 'yearly', 'intraday', 'minute',
                                         'hourly']):
            self.logger.warning("Dukascopy loader is for tick data only")

            return None

        # assume one ticker only (MarketDataGenerator only calls one ticker at a time)
        if (market_data_request.freq in ['tick']):
            # market_data_request_vendor.tickers = market_data_request_vendor.tickers[0]

            data_frame = self.get_tick(market_data_request)

            if data_frame is not None: data_frame.tz_localize('UTC')

        self.logger.info("Completed request from Dukascopy")

        return data_frame


    def get_tick(self, market_data_request):

        data_frame = self.download_tick(market_data_request)

        # convert from vendor to pycaishen tickers/fields

        returned_fields = []
        returned_tickers = []

        if data_frame is not None:
            returned_fields = data_frame.columns
            returned_tickers = [market_data_request.tickers[0]] * (len(returned_fields))

        if data_frame is not None:
            fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        return data_frame

    def download_tick(self, market_data_request):

        symbol = market_data_request.tickers[0]
        df_list = []

        self.logger.info("About to download from Dukascopy... for " + symbol)

        # single threaded
        df_list = [self.fetch_file(time, symbol) for time in
                  self.hour_range(market_data_request.start_date, market_data_request.finish_date)]



        try:
            return pandas.concat(df_list)
        except:
            return None

    def fetch_file(self, time, symbol):
        if time.hour % 24 == 0: self.logger.info("Downloading... " + str(time))

        tick_path = self.tick_name.format(
                symbol = symbol,
                year = str(time.year).rjust(4, '0'),
                month = str(time.month).rjust(2, '0'),
                day = str(time.day).rjust(2, '0'),
                hour = str(time.hour).rjust(2, '0')
            )

        tick = self.fetch_tick(Dukas_Settings.DUKASCOPY_BASE_URL + tick_path)

        if Dukas_Settings.DUKASCOPY_WRITE_TEMP_TICK_DISK:
            out_path = Dukas_Settings.TEMP_FOLDER + "/dkticks/" + tick_path

            if not os.path.exists(out_path):
                if not os.path.exists(os.path.dirname(out_path)):
                    os.makedirs(os.path.dirname(out_path))

            self.write_tick(tick, out_path)

        try:
            return self.retrieve_df(lzma.decompress(tick), symbol, time)
        except:
            return None

    def fetch_tick(self, tick_url):
        i = 0
        tick_request = None

        # try up to 5 times to download
        while i < 5:
            try:
                tick_request = requests.get(tick_url)
                i = 5
            except:
                i = i + 1

        if (tick_request is None):
            self.logger("Failed to download from " + tick_url)
            return None

        return tick_request.content

    def write_tick(self, content, out_path):
        data_file = open(out_path, "wb+")
        data_file.write(content)
        data_file.close()

    def chunks(self, list, n):
        if n < 1:
            n = 1
        return [list[i:i + n] for i in range(0, len(list), n)]

    def retrieve_df(self, data, symbol, epoch):
        date, tuple = self.parse_tick_data(data, epoch)

        df = pandas.DataFrame(data = tuple, columns=['temp', 'ask', 'bid', 'askv', 'bidv'], index = date)
        df.drop('temp', axis = 1)
        df.index.name = 'Date'

        divisor = 100000

        # where JPY is the terms currency we have different divisor
        if symbol[3:6] == 'JPY':
            divisor = 1000

        # prices are returned without decimal point
        df['bid'] =  df['bid'] /  divisor
        df['ask'] =  df['ask'] / divisor

        return df

    def hour_range(self, start_date, end_date):
          delta_t = end_date - start_date

          delta_hours = (delta_t.days *  24.0) + (delta_t.seconds / 3600.0)

          for n in range(int (delta_hours)):
              yield start_date + timedelta(0, 0, 0, 0, 0, n) # Hours

    def parse_tick_data(self, data, epoch):
        import struct

        # tick = namedtuple('Tick', 'Date ask bid askv bidv')

        chunks_list = self.chunks(data, 20)
        parsed_list = []
        date = []

        # note: Numba can speed up for loops
        for row in chunks_list:
            d = struct.unpack(">LLLff", row)
            date.append((epoch + timedelta(0,0,0, d[0])))


            parsed_list.append(d)

        return date, parsed_list

