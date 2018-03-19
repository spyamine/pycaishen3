from pycaishen.user_programs.Quandl.Quandl_data import QuandlData
from pycaishen.user_programs.Quandl.Quandl_tickers import QuandlTickers
from pycaishen.user_programs.user_programs_settings import Configurer
from pycaishen.pycaishenstorage import PycaishenStorage

from time import sleep

def _chunks(list, n):
    """Yield successive n-sized chunks from l."""

    if n > 1:
        for i in range(0, len(list), n):
            yield list[i:i + n]
    elif n == 1:
        for i in range(0, len(list)):
            yield list[i:i + 1]


def Quandl_tickers_download(Quandl_DB):
    """
    get's the tickers list for each quandl database and store it in database named
    Configurer.QUANDL_SYMBOL_LIBRARY = "Quandl.Database.symbols"
    :param Quandl_DB: list of Quandl databases
    :return: none
    """
    Quandl_ticker = QuandlTickers()
    data = Quandl_ticker.get(Quandl_DB)
    Quandl_ticker.save(data)

def get_Quandl_data(Quandl_DB_name,batch_size_per_download=100):

    """
    gets the quandl data from the Quandl api and store the data in a library named = "QUANDL." + Quandl_DB_name
    :param Quandl_DB_name:
    :param batch_size_per_download: 100
    :return:
    """

    q_data = QuandlData()
    symbols = q_data.get_quandl_tickers_from_store(Quandl_DB_name)

    print((len(symbols)))

    library = "Quandl." + Quandl_DB_name

    for symbols in _chunks(symbols, batch_size_per_download):
        data = q_data.get_Quandl(symbols)
        q_data.store_Quandl_data(data, library)
        sleep(3)

def get_unloaded_Quandl_data(Quandl_DB_name,batch_size_per_download = 100):

    q_data = QuandlData()
    # get all the tickers of the quandl database
    full_symbols = q_data.get_quandl_tickers_from_store(Quandl_DB_name)

    #connect to the database and get the symbols loaded
    storage = PycaishenStorage("arctic")
    library = "Quandl." + Quandl_DB_name

    downloaded_symbols = storage.list_symbols(library)
    # get the symbol not available in the database

    print("figuring out the symbols to download ...")

    symbols_to_dowload= list(set(full_symbols)-set(downloaded_symbols))
    print((len(symbols_to_dowload)))
    q_data = QuandlData()



    for symbols in _chunks(symbols_to_dowload, batch_size_per_download):
        data = q_data.get_Quandl(symbols)
        q_data.store_Quandl_data(data, library)
        sleep(3)



#TODO add routine to get data from start date to end date for regular updates



if __name__ == '__main__':

    # get all quandl tickers
    # one time operation
    all_Quandl_db = Configurer.QUANDL_LIBRARIES_LIST
    Quandl_tickers_download(all_Quandl_db)

    # get Quandl data for each library in the Quandl library

    # batch_size_per_download = 1000
    # Quandl_db_names_list = all_Quandl_db

    # Quandl_db_names_list =  Quandl_db_names_list[2:]

    # for db_name in Quandl_db_names_list:
    #     print(("Working on: %s " , db_name))
    #     get_Quandl_data(db_name,batch_size_per_download)

    #
    #

    # get Quandl data not loaded for each Quandl library

    # for db_name in Quandl_db_names_list:
    #     get_unloaded_Quandl_data(db_name,batch_size_per_download)

    # storage = PycaishenStorage("arctic")
    #
    #
    # libraries = storage.list_libraries()
    # print libraries