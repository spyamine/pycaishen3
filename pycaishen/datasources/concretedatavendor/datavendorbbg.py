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

"""
LoaderBBG

Abstract class for download of Bloomberg daily, intraday data and reference data.

Implemented by
- LoaderBBGOpen (adapted version of new Bloomberg Open API for Python - recommended - although requires compilation)

"""

from pycaishen.datasources.idatasource import IDataSource
import abc
from pycaishen.util.loggermanager import LoggerManager
# from pycaishen.datasources.datasourcesoptions import OptionsBBG


from pycaishen.datasources.concretedatavendor.BBG import BBGLowLevelHistorical
from pycaishen.datasources.concretedatavendor.BBG import BBGLowLevelRef
from pycaishen.datasources.concretedatavendor.BBG import BBGLowLevelTick
from pycaishen.datasources.concretedatavendor.BBG import BBGLowLevelIntraday


class DataSourceBBG(IDataSource):

    def __init__(self):
        super(DataSourceBBG, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        """
        load_ticker - Retrieves pybbg-test data from external data source (in this case Bloomberg)

        Parameters
        ----------
        market_data_request : MarketDataRequest
            contains all the various parameters detailing time series start and finish, tickers etc

        Returns
        -------
        DataFrame
        """
        # market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        data_frame = None
        self.logger.info("Request Bloomberg data")

        data_frame_list = self.get_data(market_data_request)

        self.logger.info("Completed request from Bloomberg.")

        return data_frame_list

    def _intraday_tick_dataframe_treatment(self,data_frame,market_data_request):
        if data_frame is not None:
            if data_frame.empty:
                try:
                    self.logger.info("No tickers returned for: " + market_data_request.tickers)
                except:
                    pass

                return None

            cols = data_frame.columns.values

            import pytz

            try:
                data_frame = data_frame.tz_localize(pytz.utc)
            except:
                data_frame = data_frame.tz_convert(pytz.utc)

            cols = market_data_request.tickers[0] + "." + cols
            data_frame.columns = cols

            if data_frame is not None:
                if data_frame.empty == False:
                    data_frame.index.name = 'Date'
                    data_frame = data_frame.astype('float32')

            return  data_frame

    def _reference_dataframe_treatment(self, data_frame, market_data_request):

        if data_frame is not None:
            if data_frame.empty:
                try:
                    self.logger.info("No tickers returned for: " + market_data_request.tickers)
                except:
                    pass

                return None

            cols = data_frame.columns.values
            cols = market_data_request.tickers[0] + "." + cols
            data_frame.columns = cols

            if hasattr(market_data_request,"timeseries_type"):
                if market_data_request.timeseries_type == True:
                    try:
                        data_frame_save = data_frame
                        data_frame.index.name = 'Date'
                    except:
                        return data_frame_save

            return data_frame

    def get_data(self,market_data_request):


        # decompose the market data request to respect the limitations on the max number of fields and tickers per request
        market_data_request_list = self.decompose_market_data_request(market_data_request, "bloomberg")
        print(("tickers: " , len(market_data_request.tickers)))
        try:
            print(("fields : " ,len(market_data_request.fields)))
        except:
            print("empty fields")
        print(("all requests: ", len(market_data_request_list)))
        data_frame_agg = []

        # Looping through the market data request list
        for  market_data_request in market_data_request_list:

            REFERENCE_DATA = False
            if hasattr(market_data_request,"category"):
                if market_data_request.category == 'reference' :
                    REFERENCE_DATA = True
                    data_frame = self._get_reference_data(market_data_request)

                    data_frame = self._reference_dataframe_treatment(data_frame,market_data_request)

                    data_frame_agg.append(data_frame)

            if hasattr(market_data_request,"freq") and REFERENCE_DATA == False:
                if (market_data_request.freq in ['daily', 'weekly', 'monthly', 'quarterly', 'yearly','']):
                    data_frame = self._get_historical_data(market_data_request)

                    data_frame_agg.append(data_frame)

                elif (market_data_request.freq in ['tick', 'intraday', 'second', 'minute', 'hourly']):
                    if market_data_request.freq in ['tick', 'second']:
                        data_frame = self.download_tick(market_data_request)
                        data_frame = self._intraday_tick_dataframe_treatment(data_frame,market_data_request)
                        data_frame_agg.append(data_frame)
                    else:
                        data_frame = self.download_intraday(market_data_request)
                        data_frame = self._intraday_tick_dataframe_treatment(data_frame, market_data_request)
                        data_frame_agg.append(data_frame)







        self.logger.info("Completed request from Bloomberg.")

        return data_frame_agg


    def _get_historical_data(self, market_data_request):
        data_frame = self.download_daily(market_data_request)

        # convert from vendor to pycaishen tickers/fields
        if data_frame is not None:
            if data_frame.empty:
                self.logger.info("No tickers returned for...")

                try:
                    self.logger.info(str(market_data_request.tickers))
                except: pass

                return None

            returned_fields = data_frame.columns.get_level_values(0)
            returned_tickers = data_frame.columns.get_level_values(1)


            try:
                fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            except:
                self.logger.warn("translate_from_vendor_field failed No fields returns probably!!")


            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        return data_frame

    def _get_reference_data(self, market_data_request):

        # TODO :work on this function and see how it works
        # end = datetime.utcnow()
        #
        # from datetime import timedelta
        # end = end + timedelta(days=365)# end.replace(year = end.year + 1)
        #
        # market_data_request.finish_date = end

        self.logger.debug("Requesting ref for " + market_data_request.tickers[0] + " etc.")

        data_frame = self.download_ref(market_data_request)

        self.logger.debug("Waiting for ref...")



        return data_frame

    # implement method in abstract superclass
    @abc.abstractmethod
    def kill_session(self):
        return

    @abc.abstractmethod
    def download_tick(self, market_data_request):
        return

    @abc.abstractmethod
    def download_intraday(self, market_data_request):
        return

    @abc.abstractmethod
    def download_daily(self, market_data_request):
        return

    @abc.abstractmethod
    def download_ref(self, market_data_request):
        return

#######################################################################################################################

"""
LoaderBBGOpen

Calls the Bloomberg Open API to download market data: daily, intraday and reference data (needs blpapi).

"""

import abc
import copy
#
import datetime
# import re
from datetime import datetime

from pycaishen.util.loggermanager import LoggerManager
try:
    import blpapi   # obtainable from Bloomberg website
except: pass


class DataVendorBBGOpen(DataSourceBBG):
    def __init__(self):
        super(DataVendorBBGOpen, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    def download_tick(self, market_data_request):
        # Bloomberg OpenAPI implementation
        low_level_loader = BBGLowLevelTick()

        # by default we download all available fields!
        data_frame = low_level_loader.load_time_series(market_data_request)

        # self.kill_session() # need to forcibly kill_session since can't always reopen



        return data_frame


    def download_intraday(self, market_data_request):
        # Bloomberg OpenAPI implementation
        low_level_loader = BBGLowLevelIntraday()

        # by default we download all available fields!
        data_frame = low_level_loader.load_time_series(market_data_request)

        # self.kill_session() # need to forcibly kill_session since can't always reopen

        return data_frame

    def download_daily(self, market_data_request):
        # Bloomberg Open API implementation
        low_level_loader = BBGLowLevelHistorical()

        # by default we download all available fields!
        data_frame = low_level_loader.load_time_series(market_data_request)

        # self.kill_session() # need to forcibly kill_session since can't always reopen

        return data_frame

    def download_ref(self, market_data_request):

         # Bloomberg Open API implementation
        low_level_loader = BBGLowLevelRef()

        # market_data_request_vendor_selective = copy.copy(market_data_request)

        data_frame =  low_level_loader.load_time_series(market_data_request)
        # print "dataframe : "
        # print data_frame
        # self.kill_session() # need to forcibly kill_session since can't always reopen

        return data_frame

    def kill_session(self):
        # TODO not really needed, because we automatically kill sessions
        BBGLowLevelHistorical().kill_session(None)
        BBGLowLevelRef().kill_session(None)
        BBGLowLevelIntraday().kill_session(None)






