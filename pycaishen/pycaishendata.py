# import os
# ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/') + "/"
# print ROOT_FOLDER

# current_folder = ROOT_FOLDER + "pycaishen"
# import sys
# sys.path.append(current_folder)

# import sys
# print(sys.path)


import logging

from pycaishen.util.loggermanager import LoggerManager
from pycaishen.util.settings import configurer as SETTINGS
from pycaishen.market_requests.marketdatafetcher import MarketDataFetcher


from pycaishen.market_requests.marketdatarequest import MarketDataRequestFactory
from pycaishen.datasources.datasourcesoptions import DataSourceOptionsFactory

class PycaishenData(object):
    """
    The Pycaishen class is a top-level God object. Responsible for handling different datasources
    Available datasources :
        1- Bloomberg : bloomberg
        2- Quandl : Quandl
        3- pandas_datareader.data :
    """

    FETCHER = MarketDataFetcher()
    REQUEST_READY = False

    def __init__(self,app_name=SETTINGS.APPLICATION_NAME):

        self._application_name = app_name
        self._logger = LoggerManager().getLogger(__name__)
        self.datasource_options = None





    def __str__(self):
        return "<Pycaishen Data at %s>" % (hex(id(self)))

    def __repr__(self):
        return str(self)



    def set_request(self, datasource_name, data_source_tickers, data_source_fields=None,
                 start_date = None, finish_date= None,
                 tickers =None, fields=None, category=None, freq="", timeseries_type = True):
        # set request and then it will be retrieved from the datasource

        market_data_request = MarketDataRequestFactory()
        md_request_name = SETTINGS.DEFAULT_MARKET_DATA_REQUEST

        # checking datasource_name if it's a valid datasoource
        datasource_name = self._check_data_source_name(datasource_name)

        self.FETCHER.setMarketDataRequest(md_request_name, data_source = datasource_name, data_source_fields=data_source_fields,
                                fields=fields, category=category, data_source_tickers = data_source_tickers,
                                start_date=start_date, finish_date=finish_date, freq=freq, timeseries_type=timeseries_type,
                                datasource_options=self.datasource_options, tickers=tickers)
        self.REQUEST_READY = True




    def _check_data_source_name(self,datasource_name):
        if datasource_name is not None:
            if type(datasource_name) == type("str"):
                datasource_name = str(datasource_name).lower()
                if datasource_name in SETTINGS.VALID_DATASOURCE:
                    return datasource_name
                else :
                    self._logger.warn("Invalid datasource name provided")
                    self._logger.info("Valid datasources are : %" % SETTINGS.VALID_DATASOURCE.strip('[]'))
                    return ""

    def fetch_request(self):
        # fetch the request from the data source
        if self.REQUEST_READY:
            return self.FETCHER.fetch_market_data()
        else :
            self._logger.warn("Please set a request before trying to fetch the data ... ")
            return None

    def set_datasource_options(self,datasource_name,**kwargs):

        datasourceoptions = DataSourceOptionsFactory()

        datasourceoptions = datasourceoptions(datasource_name)

        datasourceoptions.set_parameters(**kwargs)

        self.datasource_options = datasourceoptions


    def clean_datasource_options(self):

        self.datasource_options = None

