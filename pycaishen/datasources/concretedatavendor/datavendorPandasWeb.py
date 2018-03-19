from pycaishen.datasources.idatasource import IDataSource
from pycaishen.util.loggermanager import LoggerManager
import pandas

"""
DataSourcePandasWeb

Class for reading in data from various web sources into Pyfindatapy library including

- Yahoo! Finance - yahoo
- Google Finance - google
- St. Louis FED (FRED) - fred
- Kenneth French data library - famafrench
- World Bank - wb

"""

import pandas_datareader.data as web



class DataSourcePandasWeb(IDataSource):

    def __init__(self):
        super(DataSourcePandasWeb, self).__init__()
        self.logger = LoggerManager().getLogger(__name__)

    # implement method in abstract superclass
    def load_ticker(self, market_data_request):

        self.logger.info("Request Pandas Web data")

        data_frame = self.get_data(market_data_request)

        if market_data_request.data_source == 'fred':
            returned_fields = ['close' for x in data_frame.columns.values]
            returned_tickers = data_frame.columns.values
        else:
            data_frame = data_frame.to_frame().unstack()

            if data_frame.index is []: return None

            # convert from vendor to pycaishen tickers/fields
            if data_frame is not None:
                returned_fields = data_frame.columns.get_level_values(0)
                returned_tickers = data_frame.columns.get_level_values(1)

        if data_frame is not None:
            fields = self.translate_from_vendor_field(returned_fields, market_data_request)
            tickers = self.translate_from_vendor_ticker(returned_tickers, market_data_request)

            ticker_combined = []

            for i in range(0, len(fields)):
                ticker_combined.append(tickers[i] + "." + fields[i])

            ticker_requested = []

            for f in market_data_request.fields:
                for t in market_data_request.tickers:
                    ticker_requested.append(t + "." + f)

            data_frame.columns = ticker_combined
            data_frame.index.name = 'Date'

            # only return the requested tickers
            data_frame = pandas.DataFrame(data = data_frame[ticker_requested],
                                          index = data_frame.index, columns = ticker_requested)

        self.logger.info("Completed request from Pandas Web.")

        return data_frame

    def get_data(self, market_data_request):
        return web.DataReader(market_data_request.tickers, market_data_request.data_source, market_data_request.start_date, market_data_request.finish_date)

########################################################################################################################
