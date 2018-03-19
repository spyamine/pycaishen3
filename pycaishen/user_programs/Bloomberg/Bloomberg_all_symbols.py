

from pycaishen.pycaishenstorage import PycaishenStorage

from pycaishen.user_programs.user_programs_settings import Configurer

bloomberg_references = [Configurer.LIB_INDEX_COMPOSITION,Configurer.LIB_BLOOMBERG_OPEN_FIGI]
# bloomberg_references = [Configurer.LIB_INDEX_COMPOSITION]

from pycaishen.user_programs.Bloomberg.Bloomberg_currencies import build_currencies_need

def bloomberg_all_symbol(with_currency = True,with_commodity= True ,bloomberg_references_db_tables= bloomberg_references):

    print(("working on those tables %s" % bloomberg_references_db_tables))

    storage = PycaishenStorage("arctic")

    all_symbols =[]

    for lib in bloomberg_references_db_tables:
        print(("working on %s" % lib))
        symbols = storage.list_symbols(lib)

        for symbol in symbols:
            data = storage.read(symbol,lib)
            data = data["Symbol"].tolist()
            all_symbols = all_symbols + data

    if with_commodity == False:
        all_symbols = [ x for x in all_symbols if "Equity" in x]

    # add Indexes
    all_symbols = all_symbols + Configurer.AFRICAN_INDEXES

    #add currencies
    if with_currency:
        all_symbols = all_symbols + build_currencies_need()

    # remove all duplicates
    all_symbols = list(set(all_symbols))

    return all_symbols




if __name__ == '__main__':

    tickers=  bloomberg_all_symbol(with_currency=False,with_commodity=False)
    print(tickers)
    print((len(tickers)))

    # all_symbol =["ADH MC Equity","BCE MC Equity","lol Comdty"]
    #
    # all_symbol = [x for x in all_symbol if "Equity" in x]
    # print all_symbol

    pass



