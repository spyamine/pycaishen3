from pycaishen.pycaishenstorage import PycaishenStorage

from pycaishen.user_programs.user_programs_settings import Configurer

def equity_currencies( metadata_DB=Configurer.LIB_BLOOMBERG_METADATA):


    storage = PycaishenStorage("arctic")

    lib_metadata = Configurer.LIB_BLOOMBERG_METADATA
    tickers = storage.list_symbols(lib_metadata)
    # remove duplicates
    tickers = list(set(tickers))

    tickers_currency = []
    for ticker in tickers:
        # print ticker
        currency = storage.read(ticker, metadata_DB)["CURRENCY"].tolist()[0]
        tickers_currency.append(currency)

    # below a dictionnary holding for each security the corresponding currency
    return dict(list(zip(tickers, tickers_currency)))

def build_currencies_need(anchor_currencies= ["USD","EUR","GBP","JPY"],metadata_DB = Configurer.LIB_BLOOMBERG_METADATA):
    currencies_tickers = []


    storage = PycaishenStorage("arctic")

    # lib_metadata = Configurer.LIB_BLOOMBERG_METADATA
    tickers =  storage.list_symbols(metadata_DB)
    #remove duplicates
    tickers = list(set(tickers))



    all_tickers_currency = []
    for ticker in tickers:
        # print ticker
        try:
            data = storage.read(ticker, metadata_DB)
            currency =  data["CURRENCY"].tolist()[0]
            all_tickers_currency.append(currency)
        except:
            print(("Error reading data for %s" % ticker))


    # remove duplicates from the list
    all_tickers_currency = list(set(all_tickers_currency))


    # build currencies tickers for anchor currencies

    for cur in anchor_currencies:
        #remove the anchor currency from the list
        intermediate_currencies = [x for x in all_tickers_currency if x != cur]
        #create a valid bloomberg ticker
        _currencies = [ str(cur).upper() + str(x).upper() + " Curncy" for x in intermediate_currencies ]

        currencies_tickers = currencies_tickers + _currencies

    # add anchor currencies

    _currencies = []
    _currencies_tickers= []
    for cur in anchor_currencies:
        # remove the anchor currency from the list
        intermediate_currencies = [x for x in anchor_currencies if x != cur]
        # create a valid bloomberg ticker
        _currencies = [str(cur).upper() + str(x).upper() + " Curncy" for x in intermediate_currencies]
        _currencies_tickers = _currencies_tickers + _currencies

    currencies_tickers = currencies_tickers + _currencies_tickers

    #remove duplicates
    currencies_tickers = list(set(currencies_tickers))
    debug = False
    if debug:
        print((len(currencies_tickers)))
        print(currencies_tickers)

    return currencies_tickers

if __name__ == '__main__':

    currencies_ticker = build_currencies_need()
    print(currencies_ticker)