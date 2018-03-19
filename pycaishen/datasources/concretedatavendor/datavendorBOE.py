from pycaishen.datasources.idatasource import IDataSource
from pycaishen.util.loggermanager import LoggerManager





class DataSourceBOE(IDataSource):

    def __init__(self):
        super(DataSourceBOE, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):

        # market_data_request_vendor = self.construct_vendor_market_data_request(market_data_request)

        self.logger.info("Request BOE data")

        data_frame = self.get_data(market_data_request)

        if data_frame is None or data_frame.index is []: return None

        # convert from vendor to pycaishen tickers/fields
        returned_tickers = []
        if data_frame is not None:
            returned_tickers = data_frame.columns

        if data_frame is not None:
            # tidy up tickers into a format that is more easily translatable
            # we can often get multiple fields returned (even if we don't ask for them!)
            # convert to lower case
            returned_fields = [(x.split(' - ')[1]).lower().replace(' ', '-') for x in returned_tickers]
            returned_fields = [x.replace('value', 'close') for x in returned_fields]  # special case for close

            returned_tickers = [x.replace('.', '/') for x in returned_tickers]
            returned_tickers = [x.split(' - ')[0] for x in returned_tickers]

            fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

        self.logger.info("Completed request from BOE.")

        return data_frame

    def get_data(self, market_data_request):
        trials = 0

        data_frame = None

        while (trials < 5):
            try:
                # TODO

                break
            except:
                trials = trials + 1
                self.logger.info("Attempting... " + str(trials) + " request to download from BOE")

        if trials == 5:
            self.logger.error("Couldn't download from ONS after several attempts!")

        return data_frame

#######################################################################################################################
