
from pycaishen.pycaishendata import PycaishenData
from pycaishen.pycaishenstorage import PycaishenStorage

from pycaishen.user_programs.user_programs_settings import Configurer


class BloombergMetadata(object):

    def __init__(self, tickers, metadata_fields=Configurer.BLOOMBERG_METADATA_FIELDS):
        self.tickers = tickers
        self.metadata_fields = metadata_fields

    def get_metadata(self):
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

    def storeTickersMetadata(self,dataframe_list,library = Configurer.LIB_BLOOMBERG_METADATA):
        import datetime
        date_update = datetime.date.today()

        print("* Storing data in the database")
        storage = PycaishenStorage("arctic")
        i = 0
        for composition in dataframe_list:
            symbol =  self._get_symbol_from_dataframe(composition)
            composition = self._remove_tickers_name_from_dataframe(composition)
            composition["Date Update"]=date_update
            composition = composition.set_index(["Date Update"])

            storage.write(symbol,composition,library,append_data=False)
            i = i + 1

        print(("=>  %d / %d tickers metadata stored successfully " % (i, len(dataframe_list))))

if __name__ == '__main__':

    storage = PycaishenStorage("arctic")
    lib_metadata = Configurer.LIB_BLOOMBERG_METADATA
    # storage.delete_library("Bloomberg.tickers.metadata")
    #
    from pycaishen.user_programs.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol

    tickers = bloomberg_all_symbol()
    tickers = Configurer.AFRICAN_INDEXES
    print((len(tickers)))

    from pycaishen.user_programs.utilities import _chunks

    batch_size_per_download=100
    progress = 0
    for tickers_chunk in _chunks(tickers, batch_size_per_download):

        progress = progress + len(tickers_chunk)
        Metadata = BloombergMetadata(tickers_chunk)

        meta = Metadata.get_metadata()
        Metadata.storeTickersMetadata(meta)
        print(("progress: %d over %d" % (progress,len(tickers))))


    # lib_metadata = Configurer.LIB_BLOOMBERG_METADATA
    # tickers =  storage.list_symbols(lib_metadata)
    #
    # for ticker in tickers:
    #     print storage.read(ticker,lib_metadata)




