from pycaishen.pycaishendata import PycaishenData
from pycaishen.pycaishenstorage import PycaishenStorage
import datetime
from pycaishen.user_programs.user_programs_settings import Configurer
storage = PycaishenStorage("arctic")
# storage.delete_library("Bloomberg.tickers.metadata")




class BloombergFundametals(object):

    def get_bloomberg_Fundametals(self, bloomberg_symbols_tickers,datasource_fields,start_date = "01 01 2000",finish_date = None,
                          ):

        if finish_date == None:
            finish_date = datetime.date.today() - datetime.timedelta(days=1)

        data = PycaishenData()
        datasource = "bloomberg"

        data.set_request(datasource_name=datasource, data_source_fields=datasource_fields,
                         category=None, data_source_tickers=bloomberg_symbols_tickers,
                          start_date=start_date, finish_date=finish_date, freq="daily", timeseries_type=True,
                          )
        print(("* getting data for %d symbols ....." % (len(bloomberg_symbols_tickers))))
        return data.fetch_request()



    def store_bloomberg_Fundamentals(self,list_dataframes,library = Configurer.LIB_BLOOMBERG_FUNDAMENTAL_DATA):

        print("* Storing data in the database")
        storage = PycaishenStorage("arctic")

        i = 0
        for dataframe in list_dataframes:
            symbol = self._get_symbol_from_dataframe(dataframe)
            storage.write(symbol,dataframe,library)
            # print "%s EOD data stored successfully " % (symbol)
            i = i + 1
        print(("=>  %d / %d Fundamental data stored successfully " % (i,len(list_dataframes))))


    def _get_symbol_from_dataframe(self,dataframe):
        symbol = dataframe.columns[0].split('.')[0]
        return symbol


if __name__ == '__main__':

    from pycaishen.user_programs.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol

    # tickers = bloomberg_all_symbol()

    # print len(tickers)
    lib = Configurer.LIB_BLOOMBERG_FUNDAMENTAL_FIELDS
    symbol = "Bloomberg.Etat_detail"
    fundametal_symbols = storage.read(symbol, lib)["Id_Agregat_Bloom"].tolist()
    print((len(fundametal_symbols)))

    # tickers = tickers
    fundametal_symbols= list(set(fundametal_symbols))
    print((len(fundametal_symbols)))
    # fundametal_symbols = fundametal_symbols[0:50]
    print((len(fundametal_symbols)))
    tickers = ["BCE MC Equity","ADH MC Equity"]
    f = BloombergFundametals()
    data_l = f.get_bloomberg_Fundametals(tickers,fundametal_symbols)
    for data in data_l:
        # print data.head()
        print((data.columns))

    f.store_bloomberg_Fundamentals(data_l)
    library = "Bloomberg.Fundamentals"
    t = storage.list_symbols(library)
