__author__ = 'Mohamed Amine Guessous'


from pycaishen.datasources.idatasource import IDataSource
from pycaishen.util.loggermanager import LoggerManager


from pycaishen.util.settings import Reuters_configuration as settings

"""
Class to get the data from Reuters Eikon
"""

# support Quandl 3.x.x
try:
    import eikon as ek

except:
    # if import fails use Quandl 2.x.x
    import Eikon as ek

# setting the id of the app to get the data we need from Reuters Eikon

ek.set_app_id(settings.REUTERS_APP_ID)


class DataSourceReuters(IDataSource):

    def __init__(self):
        super(DataSourceReuters, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        """
        this method gets the data for the market_data_request
        :param market_data_request:
        :return: dataframe list containing the data
        """

        self.logger.info("Request Reuters data")

        data_frame_list = self.get_data(market_data_request)

        self.logger.info("Completed request from Reuters ")

        return data_frame_list


    def get_data(self,market_data_request):

        """
        generic function to get data depending on parameters on the market_data_request parameter
        :param market_data_request:
        :return:
        """
        # TODO add the proper limitation on the number of fields and tickers to send into one request




        market_data_request_list = self.decompose_market_data_request(market_data_request,"reuters")
        data_frame_agg = []

        for market_data_request in market_data_request_list:
            if hasattr(market_data_request,'category'):
                if market_data_request.category != "":
                    if market_data_request.category == "timeseries":
                        if hasattr(market_data_request, 'finish_date') and hasattr(market_data_request, 'start_date'):
                            data_frame = self._get_timeseries(market_data_request)

            if hasattr(market_data_request, 'finish_date') and hasattr(market_data_request, 'start_date'):


                try:
                    if market_data_request.finish_date != "" and market_data_request.start_date!="":
                        data_frame = self._get_data_with_time_specified(market_data_request)
                except:
                    bulk = True
                if bulk == False:
                    data_frame = self._get_data_with_time_specified(market_data_request)
                else:
                    data_frame = self._get_bulk_for_tickers(market_data_request)
            else:
                data_frame = self._get_bulk_for_tickers(market_data_request)


            # treatment of the data before appending
            treat = True

            if treat:
                data_frame_list = self.treat_data(data_frame, market_data_request)
            data_frame_agg.append(data_frame)




        return data_frame_agg

    def _get_timeseries(self,market_data_request):

        df = ek.get_timeseries(market_data_request.tickers,)

    def _get_reuters_data(self,market_data_request):
        pass