from pycaishen.pycaishendata import PycaishenData
# from pycaishen.util.loggermanager import LoggerManager

if __name__ == '__main__':

    def print_data(data):
        for df in data:
            print((100 * "=="))
            print((df.head(10)))
            print((100 * "**"))
            print((df.tail(10)))
            print((100 * "=="))
    def zprint_label(label=""):
        print((3*"**"+ label + 3*"**"))

    fetch = PycaishenData()


    # "**********************************************************************************************************"
    #
    # zprint_label("bloomberg: historical ")
    # datasource = "bloomberg"
    # datasource_tickers = ["ADH MC Equity", "BCP MC Equity"]
    # datasource_fields = ["PX LAST", "PX LOW"]
    # tickers = ["ADH", "BCP"]
    #
    # fetch.set_request(datasource_name=datasource, data_source_fields = datasource_fields,
    #                              fields=None,category = None,data_source_tickers=datasource_tickers,
    #                   start_date="21 01 2016", finish_date="21 12 2016",freq="daily", timeseries_type=True,
    #                              tickers = tickers)
    #
    # print_data(fetch.fetch_request())
    #
    # "**********************************************************************************************************"
    #
    # zprint_label("bloomberg: historical with dataoptions")
    #
    # datasource = "bloomberg"
    # datasource_tickers = ["ADH MC Equity", "BCP MC Equity"]
    # datasource_fields = ["PX LAST", "PX LOW"]
    # tickers = ["ADH", "BCP"]
    #
    # options_type = ['parameter', 'parameter','parameter']
    # options_fields = ["nonTradingDayFillOption", "nonTradingDayFillMethod","UseDPDF"]
    # options_values = ["ALL_CALENDAR_DAYS", "PREVIOUS_VALUE","N"]
    #
    # fetch.set_datasource_options(datasource,options_type= options_type, options_fields=options_fields, options_values=options_values)
    #
    # fetch.set_request(datasource_name=datasource, data_source_fields=datasource_fields,
    #                   fields=None, category=None, data_source_tickers=datasource_tickers,
    #                   start_date="21 01 2007", finish_date="21 12 2016", freq="daily", timeseries_type=True,
    #                   tickers=tickers)
    #
    #
    # print_data(fetch.fetch_request())
    # fetch.clean_datasource_options()
    #
    # "**********************************************************************************************************"
    #
    # zprint_label("bloomberg: tick data")
    # datasource = "bloomberg"
    # datasource_tickers = ["ADH MC Equity","BCP MC Equity"]
    # datasource_fields = None
    # tickers = ["ADH","BCP"]
    #
    #
    # fetch.set_request( datasource_name=datasource,
    #                              data_source_fields=datasource_fields,
    #                               data_source_tickers=datasource_tickers,
    #                              start_date="21 01 2016", finish_date="21 12 2016", freq="tick",
    #                              timeseries_type=True,
    #                               tickers=tickers)
    #
    # print_data(fetch.fetch_request())

    "**********************************************************************************************************"

    zprint_label("bloomberg: intraday data")
    datasource = "bloomberg"
    datasource_tickers = ["VOD LN Equity","VOD SJ Equity"]
    datasource_fields = None
    tickers = ["VOD LN","VOD SJ"]


    fetch.set_request( datasource_name=datasource,
                                 data_source_fields=datasource_fields,
                                  data_source_tickers=datasource_tickers,
                                 start_date="21 01 2016", finish_date="21 12 2016", freq="minute",
                                 timeseries_type=True,
                                  tickers=tickers)

    print_data(fetch.fetch_request())


    #
    # "**********************************************************************************************************"
    #
    # zprint_label("bloomberg: reference data")
    # datasource = "bloomberg"
    # datasource_tickers = ['SPAFRUP Index',"MOSENEW Index"]
    # datasource_fields = ["INDX_MWEIGHT_HIST"]
    # tickers = ["Africa","Morocco"]
    # category = "reference"
    #
    # fetch.set_request(datasource_name=datasource,
    #                   data_source_fields=datasource_fields,
    #                   data_source_tickers=datasource_tickers,
    #                   timeseries_type=False,
    #                   tickers=tickers, category = category,finish_date="27 09 2015")
    #
    # print_data(fetch.fetch_request())
    #
    # "**********************************************************************************************************"
    # zprint_label("quandl 1")
    #
    # from pycaishen.ioengines.ioenginefactory import IOEngineFactory
    #
    # IO = IOEngineFactory()
    # IO = IO("Arctic")
    #
    # print IO.list_libraries()
    #
    # library = "Quandl.METADATA.symbols"
    #
    # symbol = "WIKI"
    #
    # datavendor_tickers = list(IO.read(symbol, library)[:2].index.values)
    # print datavendor_tickers
    #
    #
    # fetch.clean_datasource_options()
    #
    # fetch.set_request(datasource_name="quandl",data_source_tickers=datavendor_tickers)
    #
    # print_data(fetch.fetch_request())
    #

