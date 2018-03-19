__author__ = 'saeedamen' # Saeed Amen

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




import abc
import copy
from pycaishen.util.chunks import chunks
# from pycaishen.util import ConfigManager
from pycaishen.util.settings import DataSource_configuration as settings
from pycaishen.util.deprecated_warnings import deprecated
from pycaishen.util.settings import datasources_tickers_and_fields_limitation_per_request as datasource_limitations_settings

class AbstractDataSourceFactory(object):
    pass

class DataSourceFactory(AbstractDataSourceFactory):

    def __call__(self, source):
        source = str(source).lower()
        if source in settings.VALID_DATASOURCE:
            if source == 'bloomberg':
                from pycaishen.datasources.concretedatavendor.datavendorbbg import DataVendorBBGOpen
                datasource = DataVendorBBGOpen()

            elif source == 'quandl':
                from pycaishen.datasources.concretedatavendor.datavendorQuandl import DataSourceQuandl
                datasource = DataSourceQuandl()

            elif source == 'ons':
                from pycaishen.datasources.concretedatavendor.datavendorONS import DataSourceONS
                raise NotImplementedError
                datasource = DataSourceONS()

            elif source == 'boe':
                from pycaishen.datasources.concretedatavendor.datavendorBOE import DataSourceBOE
                raise NotImplementedError
                datasource = DataSourceBOE()

            elif source == 'dukascopy':
                from pycaishen.datasources.concretedatavendor.datavendorDukasCopy import DataSourceDukasCopy
                datasource = DataSourceDukasCopy()

            elif source == "reuters":
                from pycaishen.datasources.concretedatavendor.datavendorReuters import DataSourceReuters
                datasource = DataSourceReuters()

            elif source in ['yahoo', 'google', 'fred', 'oecd', 'eurostat', 'edgar-index']:
                from pycaishen.datasources.concretedatavendor.datavendorPandasWeb import DataSourcePandasWeb
                datasource = DataSourcePandasWeb()

            # TODO add support for other data sources (like Reuters)

            return datasource
        else:
            return NotImplementedError


class IDataSource(object):
    """
    IDataSource

    Abstract class for various data source loaders.

    """

    def __init__(self):
        # self.config = ConfigManager()
        # self.config = None
        return

    @abc.abstractmethod
    def load_ticker(self, market_data_request):
        """
        load_ticker - Retrieves market data from external data source

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        DataFrame
        """

        return

        # to be implemented by subclasses

    @abc.abstractmethod
    def get_data(self,market_data_request):
        """
                generic function to get data depending on parameters on the market_data_request parameter
                :param market_data_request:
                :return:
        """

        return

    @abc.abstractmethod
    def kill_session(self):
        return

    @deprecated
    def get_data_source(self, source):
        """
        get_loader - Loads appropriate data service class

        Parameters
        ----------
        source : str
            the data service to use "bloomberg", "quandl", "yahoo", "google", "fred" etc.
            we can also have forms like "bloomberg-boe" separated by hyphens

        Returns
        -------
        DataVendor
        """

        data_vendor = None

        source = source.split("-")[0]

        if source == 'bloomberg':
            from pycaishen.datasources.concretedatavendor.datavendorbbg import DataVendorBBGOpen
            data_vendor = DataVendorBBGOpen()

        elif source == 'quandl':
            from pycaishen.datasources.concretedatavendor.datavendorQuandl import DataSourceQuandl
            data_vendor = DataSourceQuandl()

        elif source == 'ons':
            from pycaishen.datasources.concretedatavendor.datavendorONS  import DataSourceONS
            data_vendor = DataSourceONS()

        elif source == 'boe':
            from pycaishen.datasources.concretedatavendor.datavendorBOE  import DataSourceBOE
            data_vendor = DataSourceBOE()

        elif source == 'dukascopy':
            from pycaishen.datasources.concretedatavendor.datavendorDukasCopy  import DataSourceDukasCopy
            data_vendor = DataSourceDukasCopy()

        elif source in ['yahoo', 'google', 'fred', 'oecd', 'eurostat', 'edgar-index']:
            from pycaishen.datasources.concretedatavendor.datavendorPandasWeb  import DataSourcePandasWeb
            data_vendor = DataSourcePandasWeb()

        # TODO add support for other data sources (like Reuters)

        return data_vendor

    def translate_from_vendor_field(self, vendor_fields_list, market_data_request):
        """
        translate_from_vendor_field - Converts all the fields from vendors fields to pycaishen fields

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        List of Strings
        """

        data_source = market_data_request.data_source

        if isinstance(vendor_fields_list, str):
            vendor_fields_list = [vendor_fields_list]

        fields_converted = []

        # if we haven't set the configuration files for automatic configuration
        if market_data_request.data_source_fields is not None:

            dictionary = dict(list(zip(market_data_request.data_source_fields, market_data_request.fields)))

            for vendor_field in vendor_fields_list:
                try:
                    fields_converted.append(dictionary[vendor_field])
                except:
                    fields_converted.append(vendor_field)


        return fields_converted

    # translate market ticker to vendor ticker
    def translate_from_vendor_ticker(self, vendor_tickers_list, market_data_request):
        """
        translate_from_vendor_ticker - Converts all the fields from vendor tickers to pycaishen tickers

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        List of Strings
        """

        if market_data_request.data_source_tickers is not None:

            dictionary = dict(list(zip(market_data_request.data_source_tickers, market_data_request.tickers)))

            tickers_stuff = []

            for vendor_ticker in vendor_tickers_list:
                tickers_stuff.append(dictionary[vendor_ticker])

            return tickers_stuff # [item for sublist in tickers_stuff for item in sublist]

        data_source = market_data_request.data_source
        # tickers_list = market_data_request.tickers

        if isinstance(vendor_tickers_list, str):
            vendor_tickers_list = [vendor_tickers_list]

        if self.config is None: return vendor_tickers_list

        tickers_converted = []

        for vendor_ticker in vendor_tickers_list:
            tickers_converted.append(
                self.config.convert_vendor_to_library_ticker(data_source, vendor_ticker))

        return tickers_converted

    def _decompose(self,tickers_max_nbr,fields_max_nbr,market_data_request):

        data_source_tickers = None
        data_source_fields = None

        if hasattr(market_data_request, "data_source_tickers"):
            data_source_tickers = market_data_request.data_source_tickers
        if hasattr(market_data_request, "data_source_fields"):
            data_source_fields = market_data_request.data_source_fields
        market_data_request_list = []

        if hasattr(market_data_request, "category"):
            if market_data_request.category == "reference":
                tickers_max_nbr = 1
        if hasattr(market_data_request, "data_source_fields"):
            if market_data_request.data_source_fields == None:
                tickers_max_nbr = 1
            elif hasattr(market_data_request, "data_source_fields") == False:
                tickers_max_nbr = 1
            elif len(market_data_request.data_source_fields) == 0 or market_data_request.data_source_fields == "":
                tickers_max_nbr = 1

        # add the chunks for tickers
        tickers_chunks = [x for x in chunks(market_data_request.tickers, tickers_max_nbr)]

        i_tickers = 0
        # split up tickers into groups to account for limitations
        for data_source_ticker in chunks(data_source_tickers, tickers_max_nbr):

            market_data_request_single = copy.copy(market_data_request)
            market_data_request_single.data_source_tickers = data_source_ticker
            market_data_request_single.tickers = tickers_chunks[i_tickers]
            i_tickers = i_tickers + 1

            if hasattr(market_data_request, "data_source_fields"):
                if market_data_request.data_source_fields != None:
                    if len(market_data_request.data_source_fields) > 0:
                        fields_chunks = [x for x in chunks(market_data_request.fields, fields_max_nbr)]
                        i_fields = 0
                        for data_source_field in chunks(data_source_fields, fields_max_nbr):
                            market_data_request_single = copy.copy(market_data_request_single)
                            market_data_request_single.data_source_fields = data_source_field
                            market_data_request_single.fields = fields_chunks[i_fields]
                            i_fields = i_fields + 1

                            market_data_request_list.append(market_data_request_single)
                    else:
                        market_data_request_list.append(market_data_request_single)
                else:
                    market_data_request_list.append(market_data_request_single)


            else:
                market_data_request_list.append(market_data_request_single)

        return market_data_request_list

    def decompose_market_data_request(self,market_data_request,datasource_name):
        """

        :param market_data_request:
        :return: list of market_data_request to be feeded to the datavendor by batch to respect fields and tickers limitations
        """
        # TODO add the proper limitation on the number of fields and tickers to send into one request

        if datasource_name == "bloomberg":

            tickers_max_nbr = datasource_limitations_settings.BLOOMBERG_TICKERS_LIMITATION
            fields_max_nbr = datasource_limitations_settings.BLOOMBERG_FIELDS_LIMITATION
            return self._decompose(tickers_max_nbr,fields_max_nbr,market_data_request)

        elif datasource_name == "reuters":

            tickers_max_nbr = datasource_limitations_settings.REUTERES_TICKERS_LIMITATION
            fields_max_nbr = datasource_limitations_settings.REUTERS_FIELDS_LIMITATION
            return self._decompose(tickers_max_nbr, fields_max_nbr, market_data_request)

        else :
            market_data_request_list = [market_data_request]
            return market_data_request_list


#
# if __name__ == '__main__':
#
#     datasource = DataSourceFactory()
#     datasource = datasource("bloomberg")
#
#     print datasource
#
#     tickers_i = 1000
#     fields_i = 300
#
#     def generate_items(nbr):
#         import random
#         i=0
#         ret = []
#         while (i<nbr):
#             ret.append(str(random.randint(1,100)))
#             i = i+1
#
#         return ret
#     tickers = generate_items(tickers_i)
#     fields = generate_items(fields_i)
#
#     from pycaishen.market_requests.marketdatarequest import SimpleMarketDataRequest
#
#     datasource = "bloomberg"
#     datasource_tickers=["ADH MC Equity","BCP MC Equity"]
#     datasource_fields = ["PX LAST","PX LOW"]
#     # tickers = ["ADH","BCP"]
#
#     datasource_tickers = tickers
#     datasource_fields = fields
#
#
#     mq_req = SimpleMarketDataRequest(data_source=datasource, data_source_fields = datasource_fields,
#                                  fields=None,category = None,data_source_tickers=datasource_tickers,
#                                  start_date = "21 01 2016", finish_date= "21 12 2016" ,freq="daily", timeseries_type=True,
#                                  datasource_options= None ,tickers = None )
#
#     IDataSource().decompose_market_data_request(IDataSource(),mq_req,datasource)
#
#
#
#
#
#
