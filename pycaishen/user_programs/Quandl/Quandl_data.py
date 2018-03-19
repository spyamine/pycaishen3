from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.pycaishendata import PycaishenData

from pycaishen.user_programs.user_programs_settings import Configurer


def chunks(list, n):
    """Yield successive n-sized chunks from l."""

    if n > 1:
        for i in range(0, len(list), n):
            yield list[i:i + n]
    elif n == 1:
        for i in range(0, len(list)):
            yield list[i:i + 1]


class QuandlData(object):

    def get_Quandl(self, Quandl_symbols_tickers):


        data = PycaishenData()
        datasource = "quandl"
        data.set_request(datasource_name=datasource,data_source_tickers=Quandl_symbols_tickers,  timeseries_type=True)
        print(("* getting data for %d symbols ....." % (len(Quandl_symbols_tickers))))
        return data.fetch_request()

    def get_quandl_tickers_from_store(self,quandl_db_name,library=Configurer.QUANDL_SYMBOL_LIBRARY):

        storage = PycaishenStorage("arctic")

        symbols = storage.read(quandl_db_name,library)["Symbol"].tolist()

        return symbols

    def store_Quandl_data(self,list_dataframes,library = ""):

        print("* Storing data in the database")
        storage = PycaishenStorage("arctic")

        i = 0
        for dataframe in list_dataframes:
            symbol = self._get_symbol_from_dataframe(dataframe)

            if dataframe.empty == False:
                storage.write(symbol,dataframe,library)
            else:
                print(("empty dataframe for: %s" % symbol))
            i = i + 1
        print(("=>  %d / %d quandl data stored successfully " % (i,len(list_dataframes))))


    def _get_symbol_from_dataframe(self,dataframe):
        symbol = dataframe.columns[0].split('.')[0]
        return symbol


if __name__ == '__main__':

    symbols = []
    quandl_db_name = "WIKI"
    q_data = QuandlData()
    symbols = q_data.get_quandl_tickers_from_store(quandl_db_name)
    print(symbols)
    print((len(symbols)))
    # n = 5
    # library = "QUANDL." + quandl_db_name
    # for symbols in chunks(symbols,n):
    #     data = q_data.get_Quandl(symbols)
    #
    #     q_data.store_Quandl_data(data,library)

    # library_index_composition = "Bloomberg.Index.Composition"
    #
    # storage = PycaishenStorage("arctic")
    #
    # available_indexes = storage.list_symbols(library_index_composition)
    #
    # print available_indexes
    #
    # for index in available_indexes:
    #     symbols = symbols + storage.read(index,library=library_index_composition).ix[:,0].tolist()
    #
    # #remove duplicates
    # symbols = list(set(symbols))
    #
    # symbols = [ x + " Equity" for x in symbols ]
    #
    #
    # print symbols
    #
    # EOD = BloombergEOD()
    # n = 50
    # for symbols in chunks(symbols,n) :
    #     print "working on : "
    #     print symbols
    #     data = EOD.get_bloomberg_EOD(symbols)
    #     EOD.store_bloomberg_EOD(data)

