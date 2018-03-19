from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.pycaishendata import PycaishenData

from pycaishen.user_programs.user_programs_settings import Configurer

import datetime


def chunks(list, n):
    """Yield successive n-sized chunks from l."""

    if n > 1:
        for i in range(0, len(list), n):
            yield list[i:i + n]
    elif n == 1:
        for i in range(0, len(list)):
            yield list[i:i + 1]


class BloombergEOD(object):

    def get_bloomberg_EOD(self, bloomberg_symbols_tickers,start_date = "01 01 2000",finish_date = None,adjusted = True,
                          fields= Configurer.BLOOMBERG_EOD_FIELDS,
                          datasource_fields=Configurer.BLOOMBERG_EOD_DATASOURCE_FIELDS):

        if finish_date == None:
            finish_date = datetime.date.today() - datetime.timedelta(days=1)

        data = PycaishenData()
        datasource = "bloomberg"

        if adjusted == False:
            options_type = ['parameter']
            options_fields = ["UseDPDF"]
            options_values = ["N"]

            data.set_datasource_options(datasource, options_type=options_type, options_fields=options_fields,
                                         options_values=options_values)

        data.set_request(datasource_name=datasource, data_source_fields=datasource_fields,
                          fields=fields, category=None, data_source_tickers=bloomberg_symbols_tickers,
                          start_date=start_date, finish_date=finish_date, freq="daily", timeseries_type=True,
                          )
        print(("* getting data for %d symbols ....." % (len(bloomberg_symbols_tickers))))
        return data.fetch_request()



    def store_bloomberg_EOD(self,list_dataframes,library = Configurer.LIB_BLOOMBERG_EOD_ADJUSTED):

        print("* Storing data in the database")
        storage = PycaishenStorage("arctic")

        i = 0
        for dataframe in list_dataframes:
            symbol = self._get_symbol_from_dataframe(dataframe)
            storage.write(symbol,dataframe,library)
            # print "%s EOD data stored successfully " % (symbol)
            i = i + 1
        print(("=>  %d / %d EOD data stored successfully " % (i,len(list_dataframes))))


    def _get_symbol_from_dataframe(self,dataframe):
        symbol = dataframe.columns[0].split('.')[0]
        return symbol


if __name__ == '__main__':

    from pycaishen.user_programs.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol

    symbols = bloomberg_all_symbol()

    from pycaishen.user_programs.Bloomberg.Bloomberg_currencies import build_currencies_need
    # symbols = build_currencies_need()
    end_date = "30 01 2017"
    EOD = BloombergEOD()
    n = 500
    for symbols in chunks(symbols,n) :
        print("working on : ")
        print(symbols)
        data = EOD.get_bloomberg_EOD(bloomberg_symbols_tickers=symbols,finish_date=end_date)
        # data = EOD.get_bloomberg_EOD(bloomberg_symbols_tickers=symbols)
        EOD.store_bloomberg_EOD(data)

