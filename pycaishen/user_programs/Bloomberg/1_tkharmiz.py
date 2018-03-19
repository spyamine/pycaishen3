from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.user_programs.user_programs_settings import Configurer
#
# storage = PycaishenStorage("arctic")
#
# print storage.list_libraries()
# lib = 'Bloomberg.Fundamental.fields'
#
# print storage.list_symbols(lib)
#
# symbol = 'Bloomberg.Etat_detail'
# data = storage.read(symbol,lib)
#
# len =  data.shape[0]
# for i in range(len):
#     print data[i]
#
#
Quandl_DB_name = "FRED"

def get_quandl_tickers_from_store( quandl_db_name= Quandl_DB_name, library=Configurer.QUANDL_SYMBOL_LIBRARY):

    storage = PycaishenStorage("arctic")

    symbols = storage.read(quandl_db_name, library)["Symbol"].tolist()

    return symbols

full_symbols = get_quandl_tickers_from_store()
print((len(full_symbols)))

storage = PycaishenStorage("arctic")
library = "Quandl." + Quandl_DB_name

downloaded_symbols = storage.list_symbols(library)
# get the symbol not available in the database

print("figuring out the symbols to download ...")

# symbols_to_dowload = [x for x in full_symbols if x not in downloaded_symbols ]
#
# for x in full_symbols:
#     if x not in downloaded_symbols:
#         symbols_to_dowload.append(x)

already_downloaded = []
for x in full_symbols:
    if x in downloaded_symbols:
        downloaded_symbols.remove(x)
        # print len(downloaded_symbols)
        already_downloaded.append(x)

for x in already_downloaded:
    full_symbols.remove(x)

symbols_to_dowload = full_symbols

print((len(symbols_to_dowload)))



