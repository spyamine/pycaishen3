
from pycaishen.user_programs.user_programs_settings import Configurer
from pycaishen.pycaishendata import PycaishenData
from pycaishen.pycaishenstorage import PycaishenStorage


class BloombergDVD(object):

    def __init__(self, tickers, metadata_fields="DVD_HIST_ALL"):
        self.tickers = tickers
        self.metadata_fields = metadata_fields

    def get_data(self):
        data_fetcher = PycaishenData()
        datasource_name = "bloomberg"
        category = "reference"
        data_source_fields = self.metadata_fields
        data_source_tickers = self.tickers
        data_fetcher.set_request(datasource_name=datasource_name, data_source_tickers= data_source_tickers,
                                 category= category ,data_source_fields=data_source_fields,timeseries_type = False)
        return data_fetcher.fetch_request()

    def _remove_tickers_name_from_dataframe(self, data_frame):
        # removing tickers name from the headers of each dataframe to prepare for a merger

        original_columns = data_frame.columns
        # remove tickers from the columns
        columns = [x.split(".")[1] for x in original_columns]
        # build a header as a dictionary
        header = dict(list(zip(original_columns, columns)))
        # rename the dataframe
        data_frame.rename(columns=header, inplace=True)

        return data_frame

    def _get_symbol_from_dataframe(self, dataframe):
        symbol = dataframe.columns[0].split('.')[0]
        return symbol

    def storeTickersData(self,dataframe_list,library = Configurer.LIB_BLOOMBERG_DVD):
        import datetime
        date_update = datetime.date.today()

        print("* Storing data in the database")
        storage = PycaishenStorage("arctic")
        i = 0
        for composition in dataframe_list:
            symbol =  self._get_symbol_from_dataframe(composition)
            composition = self._remove_tickers_name_from_dataframe(composition)
            composition["Date Update"] = date_update
            # composition = composition.set_index(["Date Update"])

            storage.write(symbol,composition,library,append_data=False)
            i = i + 1

        print(("=>  %d / %d tickers stored successfully " % (i, len(dataframe_list))))

if __name__ == '__main__':

    storage = PycaishenStorage("arctic")

    # from pycaishen.examples.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol
    #
    # tickers = bloomberg_all_symbol()
    # equity  = [ x for x in tickers if "Equity" in x]
    #
    # # other method to get only equity
    # # import re
    # #
    # # equity = [x for x in tickers if re.search("Equity" ,x)]
    # # print len(equity)
    #
    # tickers = equity
    # from pycaishen.examples.utilities import _chunks
    #
    # batch_size_per_download=100
    # progress = 0
    # for tickers_chunk in _chunks(tickers, batch_size_per_download):
    #
    #     progress = progress + len(tickers_chunk)
    #     Metadata = BloombergDVD(tickers_chunk)
    #
    #     meta = Metadata.get_data()
    #     Metadata.storeTickersData(meta)
    #     print "progress: %d over %d" % (progress,len(tickers))

    lib = Configurer.LIB_BLOOMBERG_DVD
    symbols = storage.list_symbols(lib)
    print((len(symbols)))

    # for s in symbols:
    #     print s
    #     print storage.read(s,lib)