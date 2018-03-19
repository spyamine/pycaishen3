__author__ = 'Mohamed Amine Guessous'


"""
MarketDataRequest

Provides parameters for requesting market data

"""

from pycaishen.util.loggermanager import LoggerManager
from datetime import timedelta
import datetime

from pycaishen.util.settings import Market_Request_configuration as settings
from pycaishen.datasources.datasourcesoptions import DataSourceOptionsFactory

import abc

class AbstractMarketDataRequest(object):
    """
        Interface for class that models the standard market data request features
        the features are what we need to have to formulate a proper market data request

     """

    @abc.abstractmethod
    def __init__(self, data_source, data_source_tickers,
                 tickers = None, freq = "daily",
                 datasource_options = None):

        self.data_source = data_source  # datasource
        self.data_source_tickers = data_source_tickers  # define vendor tickers

        self.freq = freq  # define frequency of data

        self.datasource_options = datasource_options
        self.logger = LoggerManager().getLogger(__name__)

        # for None case
        if tickers is None:
            self.tickers = self.data_source_tickers
        else:
            self.tickers = tickers

    def __str__(self):
        s = "Market Data request features:\n"
        s = s + str(vars(self))
        return s

    def _date_parser(self, date):
        if date == None:
            return ""
        if isinstance(date, str):
            if date == "":
                return ""

            date1 = datetime.datetime.utcnow()

            if date is 'midnight':
                date1 = datetime.datetime(date1.year, date1.month, date1.day, 0, 0, 0)
            elif date is 'decade':
                date1 = date1 - timedelta(days=360 * 10)
            elif date is 'year':
                date1 = date1 - timedelta(days=360)
            elif date is 'month':
                date1 = date1 - timedelta(days=30)
            elif date is 'week':
                date1 = date1 - timedelta(days=7)
            elif date is 'day':
                date1 = date1 - timedelta(days=1)
            elif date is 'hour':
                date1 = date1 - timedelta(hours=1)
            # elif type(date) == "str" :
            #     date1 = datetime.datetime.fromstring
            else:
                # format expected 'Jun 1 2005 01:33', '%b %d %Y %H:%M'
                try:
                    date1 = datetime.datetime.strptime(date, '%b %d %Y %H:%M')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                # format expected '1 Jun 2005 01:33', '%d %b %Y %H:%M'
                try:
                    date1 = datetime.datetime.strptime(date, '%d %b %Y %H:%M')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.date.strptime(date, '%b %d %Y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%d %m %Y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%d %m %y')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%Y %m %d')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0

                try:
                    date1 = datetime.datetime.strptime(date, '%Y %m %d')
                except:
                    # self.logger.warning("Attempted to parse date")
                    i = 0
        else:
            date1 = date

        return date1

class AbstractMarketDataRequestFactory(object):

    def __call__(self, MarketDataRequestName,**kwargs):
        raise NotImplementedError

class MarketDataRequestFactory(AbstractMarketDataRequestFactory):

    def __call__(self,MarketDataRequestName, **kwargs):

        if MarketDataRequestName == "SimpleMarketDataRequest": return SimpleMarketDataRequest(**kwargs)

        elif MarketDataRequestName == "BulkTickersMarketDataRequest": return BulkTickersMarketDataRequest(**kwargs)



        else : return SimpleMarketDataRequest(**kwargs)


class BulkTickersMarketDataRequest(AbstractMarketDataRequest):

    """
    this class models the standard market data request features we need to have to model a proper market data request
    used if you have only tickers and want to download all fields if the datasource allow that.
    You have to check of the the datasource allow that
    """


    def __init__(self, data_source, data_source_tickers,
                 tickers, freq, timeseries_type,
                 datasource_options
                 ):
        """

        :param data_source: ['ats', 'bloomberg', 'dukascopy', 'fred', 'gain', 'google', 'quandl', 'yahoo']
        :param data_source_tickers: list of valid vendor tickers
        :param tickers: (optional) to get easy to recognize tickers; default is vendor tickers
        :param freq: ['tick', 'second', 'minute', 'intraday', 'hourly', 'daily', 'weekly', 'monthly', 'quarterly', 'annually']
        :param datasource_options: constructed from DataSourceOptionsFactory. It will contains specific options in the
                                    form of a dictionary in order for the datasources to take account of this options
                                    like unadjusted, fecth from ref or other in bloomberg etc...
        """


        self.data_source = data_source # datasource
        self.data_source_tickers = data_source_tickers  # define vendor tickers
        self.timeseries_type = timeseries_type

        self.freq = freq  # define frequency of data

        self.datasource_options = datasource_options
        self.logger = LoggerManager().getLogger(__name__)


        # for None case
        if tickers is None:
            self.tickers= self.data_source_tickers
        else:
            self.tickers = tickers




    @property
    def data_source(self):
        return self.__data_source

    @data_source.setter
    def data_source(self, data_source):
        try:
            valid_data_source = settings.VALID_DATASOURCE

            if not data_source in valid_data_source:
                self.logger.warning(data_source & " is not a defined data source.")
        except: pass

        self.__data_source = data_source



    @property
    def tickers(self):
        return self.__tickers

    @tickers.setter
    def tickers(self, tickers):

        if tickers is not None:
            if not isinstance(tickers, list):
                tickers = [tickers]
        # elif tickers is None:
        #     tickers = self.data_source_tickers

        self.__tickers = tickers


    @property
    def data_source_tickers(self):
        return self.__data_source_tickers

    @data_source_tickers.setter
    def data_source_tickers(self, data_source_tickers):
        if data_source_tickers is not None:
            if not isinstance(data_source_tickers, list):
                data_source_tickers = [data_source_tickers]

        self.__data_source_tickers = data_source_tickers



    @property
    def freq(self):
        return self.__freq

    @freq.setter
    def freq(self, freq):
        freq = freq.lower()

        valid_freq = settings.VALID_FREQUENCIES
        # check if the frequencies are right and already defined
        if not freq in valid_freq:
            self.logger.warning(freq + " is not a defined frequency")

        self.__freq = freq


class SimpleMarketDataRequest(AbstractMarketDataRequest):

    """
    this class models the standard market data request features we need to have to model a proper market data request
    """


    def __init__(self, data_source, data_source_tickers, data_source_fields,
                 start_date, finish_date,
                 tickers, fields, category, freq, timeseries_type,
                 datasource_options
                 ):
        """

        :param data_source: ['ats', 'bloomberg', 'dukascopy', 'fred', 'gain', 'google', 'quandl', 'yahoo']
        :param data_source_tickers: list of valid vendor tickers
        :param data_source_fields: list of valid vendor fields
        :param start_date: default is a year before
        :param finish_date: default is today
        :param tickers: to get easy to recognize tickers; default is vendor tickers
        :param fields:  to get easy to recognize fields; default is vendor fields
        :param category:
        :param freq: ['tick', 'second', 'minute', 'intraday', 'hourly', 'daily', 'weekly', 'monthly', 'quarterly', 'annually']
        :param datasource_options: constructed from DataSourceOptionsFactory. It will contains specific options in the
                                    form of a dictionary in order for the datasources to take account of this options
                                    like unadjusted, fecth from ref or other in bloomberg etc...
        """


        self.data_source = data_source # datasource
        self.data_source_tickers = data_source_tickers  # define vendor tickers
        self.data_source_fields = data_source_fields  # define vendor fields
        self.freq = freq  # define frequency of data
        self.start_date = start_date
        self.finish_date = finish_date
        self.datasource_options = datasource_options
        self.logger = LoggerManager().getLogger(__name__)
        self.category = category  # special predefined categories
        self.timeseries_type = timeseries_type

        # for None case
        if fields is None:
            self.fields = data_source_fields
        else:
            self.fields = fields  # fields, eg. close, high, low, open
        # for None case
        if tickers is None:
            self.tickers= self.data_source_tickers
        else:
            self.tickers = tickers




    @property
    def data_source(self):
        return self.__data_source

    @data_source.setter
    def data_source(self, data_source):
        try:
            valid_data_source = settings.VALID_DATASOURCE

            if not data_source in valid_data_source:
                self.logger.warning(data_source & " is not a defined data source.")
        except: pass

        self.__data_source = data_source

    @property
    def category(self):
        return self.__category

    @category.setter
    def category(self, category):
        self.__category = category

    @property
    def tickers(self):
        return self.__tickers

    @tickers.setter
    def tickers(self, tickers):

        if tickers is not None:
            if not isinstance(tickers, list):
                tickers = [tickers]
        # elif tickers is None:
        #     tickers = self.data_source_tickers

        self.__tickers = tickers

    @property
    def fields(self):
        return self.__fields

    @fields.setter
    def fields(self, fields):

        if not isinstance(fields, list) and fields is not None:
            fields = [fields]
        # elif fields is None:
        #     fields = self.data_source_fields
        self.__fields = fields

    @property
    def data_source_tickers(self):
        return self.__data_source_tickers

    @data_source_tickers.setter
    def data_source_tickers(self, data_source_tickers):
        if data_source_tickers is not None:
            if not isinstance(data_source_tickers, list):
                data_source_tickers = [data_source_tickers]

        self.__data_source_tickers = data_source_tickers

    @property
    def data_source_fields(self):
        return self.__data_source_fields

    @data_source_fields.setter
    def data_source_fields(self, data_source_fields):
        if data_source_fields is not None:
            if not isinstance(data_source_fields, list):
                data_source_fields = [data_source_fields]

        self.__data_source_fields = data_source_fields

    @property
    def freq(self):
        return self.__freq

    @freq.setter
    def freq(self, freq):
        freq = freq.lower()

        valid_freq = settings.VALID_FREQUENCIES
        # check if the frequencies are right and already defined
        if not freq in valid_freq:
            self.logger.warning(freq + " is not a defined frequency")

        self.__freq = freq


    @property
    def start_date(self):
        return self.__start_date

    @start_date.setter
    def start_date(self, start_date):
        self.__start_date = self._date_parser(start_date)

    @property
    def finish_date(self):
        return self.__finish_date

    @finish_date.setter
    def finish_date(self, finish_date):
        self.__finish_date = self._date_parser(finish_date)

