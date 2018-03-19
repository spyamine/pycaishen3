__author__ = 'Mohamed Amine Guessous'



"""
MarketDataFetcher

Returns market data time series by directly calling data sources like Bloomberg (bloomberg), Yahoo (yahoo), Quandl (quandl),
FRED (fred) etc. which are implemented in subclasses of LoaderTemplate. This provides a common wrapper for all these data sources.

"""

import copy

# from pycaishen.timeseries import Filter, Calculations
from pycaishen.util import LoggerManager #, ConfigManager,  DataConstants,
from pycaishen.datasources.idatasource import DataSourceFactory
from pycaishen.datasources.datasourcesoptions import DataSourceOptionsFactory
from pycaishen.util.settings import Market_Request_configuration as settings

from pycaishen.datasources.datasourceuniformizer import DataOutputUniformizerFactory


class AbstractMarketDataFetcher(object):
    """
    Abstract MarketDataFectcher
    """

    def fetch_market_data(self):
        raise NotImplementedError

    def setMarketDataRequestFactory(self):
        raise NotImplementedError

class MarketDataFetcher(AbstractMarketDataFetcher):
    """
    MarketDataFetcher should get the data of the MarketDataRequest from Datavendors
    """

    _time_series_cache = {} # shared across all instances of object!

    def __init__(self):

        self.logger = LoggerManager().getLogger(__name__)
        self.market_data_request = None
        self.data_vendor = DataSourceFactory()

        return


    def setMarketDataRequest(self,*args,**kwargs):
        """

        :param args: arguments needed to initialize the MarketDataRequestFactory

        :return:
        """
        from pycaishen.market_requests.marketdatarequest import MarketDataRequestFactory
        md_request = MarketDataRequestFactory()
        md_request = md_request(*args,**kwargs)

        self.market_data_request = md_request

    def fetch_market_data(self,  kill_session = True):
        """
        fetch_market_data - Loads time series from specified data provider

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains various properties describing time series to fetched, including ticker, start & finish date etc.

        Returns
        -------
        pandas.DataFrame
        """

        market_data_request = self.market_data_request
        # initiating the datasource using the name of it in the factory we used
        datasource = self.data_vendor(market_data_request.data_source)

        data = datasource.load_ticker(market_data_request)

        uniformizer = DataOutputUniformizerFactory()

        uniformizer = uniformizer(market_data_request.data_source)()

        data = uniformizer.uniformize(data)

        return data


if __name__ == '__main__':

    def print_data_agg(fetcher):
        data_agg = fetcher.fetch_market_data()

        # print data_agg.columns
        # print data_agg
        print((type(data_agg)))

        for data in data_agg:
            print((type(data)))
            print("columns : ")
            print((data.columns))
            print("data : ")
            print((data.head(2)))
            print((data.tail(2)))

    from pycaishen.market_requests.marketdatarequest import MarketDataRequestFactory
    from pycaishen.datasources.datasourcesoptions import DataSourceOptionsFactory

    fetcher = MarketDataFetcher()
    i = 1
    print(("test: " + str(i) + 10 * "=="))
    datasource = "bloomberg"
    datasource_tickers=["ADH MC Equity","BCP MC Equity"]
    datasource_fields = ["PX LAST","PX LOW"]
    tickers = ["ADH","BCP"]


    datasourceoptions = DataSourceOptionsFactory()

    datasourceoptions = datasourceoptions(datasource)

    # options_type =['parameter','parameter']
    # options_fields = ["nonTradingDayFillOption","nonTradingDayFillMethod"]
    # options_values = ["ALL_CALENDAR_DAYS","PREVIOUS_VALUE"]
    #
    # datasourceoptions.set_parameters(options_type,options_fields,options_values)
    #
    # print datasourceoptions
    #
    # # testing historical data with bloomberg
    # fetcher.setMarketDataRequest("SimpleMarketDataRequest", data_source=datasource, data_source_fields = datasource_fields,
    #                              fields=None,category = None,data_source_tickers=datasource_tickers,
    #                              start_date = "21 01 2016", finish_date= "21 12 2016" ,freq="daily", timeseries_type=True,
    #                              datasource_options= None ,tickers = tickers )
    #
    #
    # print fetcher.market_data_request
    # print_data_agg(fetcher)

    # i = i + 1
    # print "test: " + str(i) + 10 * " == "
    #
    # # testing tick data
    # datasource = "bloomberg"
    # datasource_tickers = ["ADH MC Equity"]
    # datasource_fields = None
    # tickers = ["ADH"]
    # fetcher = MarketDataFetcher()
    #
    # fetcher.setMarketDataRequest("SimpleMarketDataRequest", data_source=datasource,
    #                              data_source_fields=datasource_fields,
    #                              fields=None, category=None, data_source_tickers=datasource_tickers,
    #                              start_date="21 01 2016", finish_date="21 12 2016", freq="tick",
    #                              timeseries_type=True,
    #                              datasource_options=None, tickers=tickers)
    #
    # print fetcher.market_data_request
    # print_data_agg(fetcher)
    #
    # i = i + 1
    # print "test: " + str(i) + 10 * " == "
    #
    # # testing tick data and intraday
    # datasource = "bloomberg"
    # datasource_tickers = ["ADH MC Equity"]
    # datasource_fields = None
    # tickers = ["ADH"]
    # fetcher = MarketDataFetcher()
    #
    # fetcher.setMarketDataRequest("SimpleMarketDataRequest", data_source=datasource,
    #                              data_source_fields=datasource_fields,
    #                              fields=None, category=None, data_source_tickers=datasource_tickers,
    #                              start_date="21 01 2016", finish_date="21 12 2016", freq="tick",
    #                              timeseries_type=True,
    #                              datasource_options=None, tickers=tickers)
    # print fetcher.market_data_request
    # print_data_agg(fetcher)

    i = i + 1
    print(("test: " + str(i) + 10 * " == "))

    # testing tick data and intraday
    datasource = "bloomberg"
    datasource_tickers = ['SPAFRUP Index']
    datasource_fields = ["INDX_MWEIGHT_HIST"]
    tickers = ["africa"]
    fetcher = MarketDataFetcher()
    category = "reference"


    options_type = ['parameter']
    options_fields = ["END_DT"]  # ["END_DT"]

    import datetime
    end_date=datetime.datetime(2013,8,12)
    print((end_date.strftime('%Y%m%d')))

    options_values = [end_date.strftime('%Y%m%d')]
    datasourceoptions.set_parameters(options_type, options_fields, options_values)
    # data.set_datasource_options(datasource, options_type=options_type, options_fields=options_fields,
    #                             options_values=options_values)

    # fetcher.setMarketDataRequest("SimpleMarketDataRequest", data_source=datasource,
    #                              data_source_fields=datasource_fields,
    #                              fields=None, category=category, data_source_tickers=datasource_tickers,
    #                              start_date="21 01 2016", finish_date="21 12 2016", freq="minute",
    #                              timeseries_type=True,
    #                              datasource_options=datasourceoptions, tickers=tickers)

    fetcher.setMarketDataRequest("SimpleMarketDataRequest", data_source=datasource,
                                 data_source_fields=datasource_fields,
                                 fields=None, category=category, data_source_tickers=datasource_tickers,
                                 start_date="", finish_date=end_date, freq="",
                                 timeseries_type=True,
                                 datasource_options=None, tickers=tickers)
    print((fetcher.market_data_request))
    print_data_agg(fetcher)


    # i = i + 1
    # print "test: " + str(i) + 10 * " == "
    #
    # from pycaishen.ioengines.ioenginefactory import IOEngineFactory
    #
    #
    # IO = IOEngineFactory()
    # IO = IO("Arctic")
    #
    #
    # print IO.list_libraries()
    #
    # library = "Quandl.METADATA.symbols"
    #
    # symbol = "WIKI"
    #
    # datavendor_tickers =  list(IO.read(symbol,library)[:10].index.values)
    # print datavendor_tickers
    # print type(datavendor_tickers)
    #
    # # , data_source, data_source_tickers,
    # # tickers, freq, timeseries_type,
    # # datasource_options

    # fetcher.setMarketDataRequest("BulkTickersMarketDataRequest",data_source="quandl",data_source_tickers = datavendor_tickers,tickers = None,freq = "",timeseries_type = False,datasource_options= None)
    #
    # print fetcher.market_data_request
    #
    #
    # print_data_agg(fetcher)
    #
    # i = i + 1
    # print "test: " + str(i) + 10 * " == "
    #
    # fetcher.setMarketDataRequest("SimpleMarketDataRequest", data_source="quandl", data_source_fields = None,
    #                              fields=None,category = None,data_source_tickers=datavendor_tickers,
    #                              start_date = "21 01 2016", finish_date= "21 12 2016" ,freq="monthly", timeseries_type=True,
    #                              datasource_options= None,tickers = None )
    #
    #
    # datasource_tickers = "ADH MC Equity"
    # datasource_fields = "PX LAST"
    #
    # print_data_agg(fetcher)
    #
    # i = i + 1
    # print "test: " + str(i) + 10 * " == "
    # fetcher.setMarketDataRequest("SimpleMarketDataRequest", data_source="bloomberg", data_source_fields=datasource_fields,
    #                              fields=None, category=None, data_source_tickers=datasource_tickers,
    #                              start_date="21 01 2016", finish_date="21 12 2016", freq="monthly",
    #                              timeseries_type=True,
    #                              datasource_options=None, tickers=None)
    #
    #
    # print fetcher.market_data_request
    #
    # print_data_agg(fetcher)