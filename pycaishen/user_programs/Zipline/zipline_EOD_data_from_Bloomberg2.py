from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.user_programs.utilities import missing_data_treatment_zero, missing_data_treatment_fillf_backfill
import pandas as pd


class AbstractZipline_EOD_Data(object):
    """
    This class represent an abstract class that show the minimum functions we need to feed the zipline engine
    Also, it holds general utility functions like treatment of missing data and else
    """
    COLUMNS_FORWARD_BACKWARD_FILL_METHOD =['mkt_cap','high', 'close', 'low', 'open']
    COLUMNS_FILL_ZERO = ['turnover', 'volume']

    def __init__(self,tickers,library,engine="arctic"):
        if type(tickers) == type("str"):
            tickers = [tickers]

        self.tickers = tickers
        self.engine = engine
        self.library = library

    def _remove_tickers_name_from_dataframe_list(self,data_frame_list):

        # removing tickers name from the headers of each dataframe to prepare for a merger
        for data_frame in data_frame_list:
            original_columns = data_frame.columns
            # remove tickers from the columns
            columns = [x.split(".")[1] for x in original_columns]
            # build a header as a dictionary
            header = dict(list(zip(original_columns, columns)))
            # rename the dataframe
            data_frame.rename(columns=header, inplace=True)

        return data_frame_list

    def _treat_dataframe_list_for_missing_data(self, data_frame_list, index_range,
                                               columns_forward_backward_fill_method=
                                               COLUMNS_FORWARD_BACKWARD_FILL_METHOD,
                                               columns_fill_zero=COLUMNS_FILL_ZERO):

        """

        :param data_frame_list:
        :param index_range:
        :param columns_forward_backward_fill_method:
        :param columns_fill_zero:
        :return:
        """
        debug = False
        if debug:
            i = 0
        data_frame_list_treated = []
        for dataframe in data_frame_list:
            if dataframe.empty == False:
                # reindex to incorporate all trading days
                # TODO Add support for benchmark reindexing
                if debug:
                    print(i)
                    i += 1

                # print "before %d" %len(dataframe)
                dataframe = dataframe.reindex(index=index_range)
                missing_data_treatment_fillf_backfill(dataframe, columns_forward_backward_fill_method)
                missing_data_treatment_zero(dataframe, columns_fill_zero)
                # print "after %d " % len(dataframe)

                data_frame_list_treated.append(dataframe)
        return data_frame_list_treated



    def load_daily_close(self, start_date=None, finish_date=None):
        raise NotImplementedError

    def load_daily_bars(self, start_date=None, finish_date=None):
        raise NotImplementedError

    def get_EOD_data_from_storage(self, start_date=None, finish_date=None):
        raise NotImplementedError


class Bloomberg_Zipline_EOD_Data(AbstractZipline_EOD_Data):
    """
    this class have a mission to transform EOD Bloomberg data taken from DB and transform it to a Panel acceptable
    by zipline
    """

    def __init__(self,tickers,library,engine="arctic"):

        if type(tickers) == type("str"):
            tickers = [tickers]

        self.tickers = tickers
        self.engine = engine
        self.library = library

    def get_EOD_data_from_storage(self, start_date=None, finish_date=None):
        """

        :param start_date:
        :param finish_date:
        :return:
        """
        debug = False
        storage = PycaishenStorage(self.engine)
        data_frame_list = []
        tickers_not_available =[]
        for ticker in self.tickers:
            # get the dataframe containing the data we need
            data = storage.read(ticker, self.library, start_date, finish_date)
            # drop the rows that are empty
            data = data.dropna(how='all')

            len_data = len(data)
            len_col = len(data.columns.tolist())
            if debug:
                print(("length of the data: %d" % len_data))
            if len_data == 0 or len_col< 7 :
                if debug:
                    print("ticker %s not available will be removed from tickers list!! ")
                tickers_not_available.append(ticker)
            else:
                data_frame_list.append(data)

        all_tickers = self.tickers
        if debug:
            print("tickers to be removed:")
            print(tickers_not_available)

        # remove tickers not available
        remaining_tickers = [x for x in all_tickers if x not in tickers_not_available]
        if debug:
            print(("old tickers lenght: %d " % len(all_tickers)))
            print(("remaining tickers lenght %d " % len(remaining_tickers)))
        # update the tickers list
        self.tickers = remaining_tickers


        return data_frame_list



    def load_daily_close(self, start_date=None, finish_date=None):
        """
        Load closing data for the stocks
        :param start_date:
        :param finish_date:
        :return: Pandas dataframe containing close data for the stocks
        """

        data_frame_list = self.get_EOD_data_from_storage(start_date, finish_date)

        # removing tickers name from the headers of each dataframe to prepare for a merger
        data_frame_list = self._remove_tickers_name_from_dataframe_list(data_frame_list)

        # treat the data for forward fill and backward fill then zero fill

        business_day_range = pd.date_range(start_date, finish_date)

        data_frame_list =self._treat_dataframe_list_for_missing_data(data_frame_list,business_day_range)


        # taking only close column from the dataframes
        close_data_series_list = []
        tickers = self.tickers
        i = 0
        for data_frame in data_frame_list:
            # print len(data_frame)
            close = data_frame.close
            # renaming the series to hold their tickers name
            close.name = tickers[i]
            close_data_series_list.append(close)
            i = i + 1

        #concatenating the differents pandas series
        data_frame = pd.concat(close_data_series_list, join='outer', axis=1)



        return data_frame


    def load_daily_bars(self, start_date=None, finish_date=None):

        data_frame_list = self.get_EOD_data_from_storage(start_date, finish_date)

        # removing tickers name from the headers of each dataframe to prepare for a merger
        data_frame_list = self._remove_tickers_name_from_dataframe_list(data_frame_list)



        # treat the data for forward fill and backward fill then zero fill

        business_day_range = pd.date_range(start_date, finish_date)

        data_frame_list = self._treat_dataframe_list_for_missing_data(data_frame_list, business_day_range)

        # adding price clomuns to match data of yahoo load bars
        for data_frame in data_frame_list:
            data_frame["price"] = data_frame["close"]

        data = dict(list(zip(self.tickers,data_frame_list)))
        # print data

        panel = pd.Panel.from_dict(data)

        # print panel

        return panel



if __name__ == '__main__':

    from datetime import datetime
    import pytz
    from zipline.utils.factory import load_from_yahoo
    from zipline.utils.factory import load_bars_from_yahoo
    start = datetime(2004, 1, 1, 0, 0, 0, 0, pytz.utc)
    end = datetime(2005, 1, 1, 0, 0, 0, 0, pytz.utc)

    STOCKS = ['AMD', 'CERN', 'COST', 'DELL', 'GPS', 'INTC', 'MMM']

    # Load price data from yahoo.
    # data = load_from_yahoo(stocks=STOCKS, indexes={}, start=start, end=end)
    # data = load_bars_from_yahoo(stocks=STOCKS, indexes={}, start=start, end=end)
    # for d in data.iteritems():
    #     print d
    # print data
    #
    # # data = data.dropna()
    # #
    # # print type(data)
    # #
    # # data = load_bars_from_yahoo(stocks=STOCKS, indexes={}, start=start, end=end)
    # # data = data.dropna()
    # #
    # # print type(data)
    #
    start = datetime(2008, 1, 1, 0, 0, 0, 0)
    end = datetime(2015, 1, 30, 0, 0, 0, 0)
    #
    STOCKS= ["BCE MC Equity","ATW MC Equity"]
    zbol = Bloomberg_Zipline_EOD_Data(STOCKS, "Bloomberg.EOD")
    data =  zbol.load_daily_bars(start, end)

    for d in list(data.items()):
        print(d)

    #
    # def get_index_tickers(index_ticker):
    #     from pycaishen.pycaishenstorage import PycaishenStorage
    #
    #     storage = PycaishenStorage("arctic")
    #     from pycaishen.examples.example_settings import Configurer
    #
    #     lib = Configurer.LIB_INDEX_COMPOSITION
    #     print storage.list_symbols(lib)
    #     return list(set(storage.read(index_ticker, lib)["Symbol"].tolist()))
    #
    # STOCKS = get_index_tickers("MOSENEW Index :")
    # print STOCKS
    # zbol = Bloomberg_Zipline_EOD_Data(STOCKS, "Bloomberg.EOD")
    # print zbol.load_daily_bars(start, end)
