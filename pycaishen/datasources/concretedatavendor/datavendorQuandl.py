
from pycaishen.datasources.idatasource import IDataSource
from pycaishen.util.loggermanager import LoggerManager


from pycaishen.util.settings import Quandl_configuration as settings


"""
DataSourceQuandl

Class for reading in data from Quandl into Pyfindatapy library

"""

# support Quandl 3.x.x
try:
    import quandl as Quandl

except:
    # if import fails use Quandl 2.x.x
    import Quandl


Quandl.ApiConfig.api_key = settings.QUANDL_API_KEY
Quandl.ApiConfig.api_version = '2015-04-09'

class DataSourceQuandl(IDataSource):

    def __init__(self):
        super(DataSourceQuandl, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):
        """
        this method gets the data for the market_data_request
        :param market_data_request:
        :return: dataframe list containing the data
        """

        self.logger.info("Request Quandl data")

        data_frame_list = self.get_data(market_data_request)

        self.logger.info("Completed request from Quandl ")

        return data_frame_list

    def _expose(self,col,label =""):
        print(label)
        for c in col:
            print(c)
        print((10*"*"))

    def treat_data(self,data_frame,market_data_request):
        """
        should return a list of dataframes where every dataframe contains all the data for each ticker
        :param data_frame:
        :return:
        """

        expose = False

        if data_frame is None or data_frame.index is []: return None

        # convert from vendor to marketdatarequest tickers/fields
        returned_tickers = []
        if data_frame is not None:
            returned_tickers = data_frame.columns

            if expose:
                self._expose(returned_tickers,"returned_tickers vanilla")


        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case

            returned_fields = [(x.split(' - ',1)[1]).lower().replace(' ', '-').replace('.', '-').replace('--', '-')
                               for x in returned_tickers]


            if expose:
                self._expose(returned_fields,"returned_fields treatment 1")

            returned_fields = [x.replace('value', 'close') for x in returned_fields]  # special case for close

            if expose:
                self._expose(returned_fields,"returned_fields treatment 2")

            #TODO understand the line below
            # replace time fields (can cause problems later for times to start with 0)
            for i in range(0, 10):
                returned_fields = [x.replace('0' + str(i) + ':00', str(i) + ':00') for x in returned_fields]

            if expose:
                self._expose(returned_fields,"returned_fields treatment 3")

            returned_tickers = [x.replace('.', '/') for x in returned_tickers]

            if expose:
                self._expose(returned_tickers,"returned_tickers treatment 1")

            returned_tickers = [x.split(' - ')[0] for x in returned_tickers]

            if expose:
                self._expose(returned_tickers,"returned_tickers treatment 2")

            try:
                if hasattr(market_data_request,"data_source_fields") and hasattr(market_data_request,"fields"):
                    fields = self.translate_from_vendor_field(returned_fields, market_data_request)
                if hasattr(market_data_request, "data_source_tickers") and hasattr(market_data_request, "tickers"):
                    tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)
            except:
                print('error')

            ticker_combined = []

            for i in range(0, len(returned_fields)):
                if tickers != None:
                    ticker_combined.append(tickers[i] + "." + returned_fields[i])
                else :
                    ticker_combined.append(returned_tickers[i] + "." + returned_fields[i])


            data_frame.columns = ticker_combined
            if market_data_request.timeseries_type:
                data_frame.index.name = 'Date'

        return data_frame

    def get_data(self,market_data_request):

        """
        generic function to get data depending on parameters on the market_data_request parameter
        :param market_data_request:
        :return:
        """
        # TODO add the proper limitation on the number of fields and tickers to send into one request




        market_data_request_list = self.decompose_market_data_request(market_data_request,"quandl")
        data_frame_agg = []

        for market_data_request in market_data_request_list:
            if hasattr(market_data_request, 'finish_date') and hasattr(market_data_request, 'start_date'):

                bulk = True
                try:
                    if market_data_request.finish_date != "" and market_data_request.start_date!="":
                        bulk = False
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



    def _get_bulk_for_tickers(self,market_data_request):
        """
        used to get all fields for market_data_request if no start or end date is specified
        :param market_data_request:
        :return:
        """
        data_frame = None
        trials = 0
        print("Market_data_request tickers : ")
        print((market_data_request.data_source_tickers))

        while (trials < settings.QUANDL_NBR_ATTENTS):
            try:
                data_frame = Quandl.get(market_data_request.data_source_tickers)
                break
            except:
                trials = trials + 1
                self.logger.info("Attempting... " + str(trials) + " request to download from Quandl")

        if trials == settings.QUANDL_NBR_ATTENTS:
            self.logger.error("Couldn't download from Quandl after several attempts!")

        return data_frame



    def _get_data_with_time_specified(self, market_data_request):

        data_frame = None
        trials = 0
        while (trials < settings.QUANDL_NBR_ATTENTS):
            try:
                if market_data_request.freq == "":
                    
                    data_frame = Quandl.get(market_data_request.data_source_tickers,

                                        start_date=market_data_request.start_date,
                                        end_date=market_data_request.finish_date)
                else :
                    data_frame = Quandl.get(market_data_request.data_source_tickers,

                                            start_date =market_data_request.start_date,
                                            end_date=market_data_request.finish_date,collapse = market_data_request.freq)
                break
            except:
                trials = trials + 1
                self.logger.info("Attempting... " + str(trials) + " request to download from Quandl")

        if trials == settings.QUANDL_NBR_ATTENTS:
            self.logger.error("Couldn't download from Quandl after several attempts!")

        return data_frame




