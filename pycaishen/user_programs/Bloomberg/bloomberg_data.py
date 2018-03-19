# general imports
from pycaishen.user_programs.user_programs_settings import Configurer
from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.pycaishendata import PycaishenData
from pycaishen.user_programs.utilities import _chunks

####################################################### part 1 ###########################################################
# indexes composition update
from pycaishen.user_programs.Bloomberg.Bloomberg_Index_Composition import BloomIndexComposition
from datetime import datetime, timedelta
# yesterday
composition_end_date =datetime.today()+timedelta(-1)
# since 2012
composition_start_date = datetime(2010, 1, 1, 0, 0, 0, 0)
# focusing on african indexes only
composition_indexes = Configurer.AFRICAN_INDEXES # ['BCOM Index']

def get_composition(indexes = composition_indexes ,start_date = composition_start_date,end_date = composition_end_date  ):

    IndexComposition = BloomIndexComposition()
    data = IndexComposition.get_composition_date(indexes, start_date, end_date)
    IndexComposition.storeIndexComposition(data)

####################################################### part 2 ###########################################################
# Open Figi tickers download
from pycaishen.user_programs.Bloomberg.Bloomberg_OpenFigi_tickers import Openfigi_constants,OpenfigiReference

def get_open_figi_data():


    import datetime

    today = datetime.date.today()

    print((str(today)))

    # Bloomberg
    exchange_list = Openfigi_constants().AFRICAN_BLOOMBERG_EXCHANGES_LIST
    target_tickers_source_name = "Bloomberg"
    reference_source_name = "openfigi"
    #
    library = Openfigi_constants().LIB_BLOOMBERG_OPEN_FIGI
    OpenFigi = OpenfigiReference()
    # exchange_list = exchange_list[0:2]
    data = OpenFigi.get_open_figi_references(exchange_list)
    OpenFigi.save_open_figi_references(data, library)

####################################################### part 3 ###########################################################
# Build tickers metadata
from pycaishen.user_programs.Bloomberg.Bloomberg_get_metadata import BloombergMetadata

def get_tickers_metadata():

    from pycaishen.user_programs.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol

    tickers = bloomberg_all_symbol(with_currency=False)




    batch_size_per_download = 100
    progress = 0
    for tickers_chunk in _chunks(tickers, batch_size_per_download):
        progress = progress + len(tickers_chunk)
        Metadata = BloombergMetadata(tickers_chunk)

        meta = Metadata.get_metadata()
        Metadata.storeTickersMetadata(meta)
        print(("progress: %d over %d" % (progress, len(tickers))))

####################################################### part 4 ###########################################################
# Build fundamental fields needed for fundamental data
from pycaishen.user_programs.Bloomberg.Bloomberg_fundamental_fields import set_Bloomberg_Fundamentals
def set_bloomberg_fundamental_fields():
    set_Bloomberg_Fundamentals()


####################################################### part 5 ###########################################################
# get EOD data for each ticker available
from pycaishen.user_programs.Bloomberg.Bloomberg_EOD import BloombergEOD

import datetime
yesterday = datetime.datetime.today() +datetime.timedelta(-1)
yesterday = yesterday.strftime("%d %m %Y")
def get_EOD_data(end_date = yesterday, adjusted = True):

    from pycaishen.user_programs.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol

    if adjusted == False:
        symbols = bloomberg_all_symbol(with_commodity=False,with_currency=False)
    else:
        symbols=bloomberg_all_symbol()


    EOD = BloombergEOD()
    n = 100
    i =0
    for symbols in _chunks(symbols,n) :
        print("working on : ")
        print(symbols)
        if adjusted == False :
            data = EOD.get_bloomberg_EOD(bloomberg_symbols_tickers=symbols,finish_date=end_date,adjusted=adjusted)
            # data = EOD.get_bloomberg_EOD(bloomberg_symbols_tickers=symbols)
            lib = Configurer.LIB_BLOOMBERG_EOD_NOT_ADJUSTED
            EOD.store_bloomberg_EOD(data,lib)
        else:
            data = EOD.get_bloomberg_EOD(bloomberg_symbols_tickers=symbols, finish_date=end_date)
            # data = EOD.get_bloomberg_EOD(bloomberg_symbols_tickers=symbols)
            EOD.store_bloomberg_EOD(data)

        i = i+1
        print(i)


####################################################### part 6 ###########################################################
# get fundamental data for each ticker available
from pycaishen.user_programs.Bloomberg.Bloomberg_get_fundamental_data import BloombergFundametals


def get_fundamental_data(size_of_chunks,chunk_to_work_on,historicals = True,estimates = True):

    from pycaishen.user_programs.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol

    tickers = bloomberg_all_symbol(with_currency=False,with_commodity=False)
    print(tickers)


    storage = PycaishenStorage("arctic")

    lib = Configurer.LIB_BLOOMBERG_FUNDAMENTAL_FIELDS
    fundametal_symbols = []
    if historicals:
        symbol = "Bloomberg.Etat_detail"

        fundametal_symbols = storage.read(symbol, lib)["Id_Agregat_Bloom"].tolist()
    if estimates:
        symbol = "Estimates"

        fundametal_symbols = fundametal_symbols + storage.read('Bloomberg.Estimates', lib)[symbol].tolist()

    fundametal_symbols = list(set(fundametal_symbols))

    print(("tickers : ",len(tickers)))
    print(("fundamentals ",len(fundametal_symbols)))
    hits = len(tickers)  * len(fundametal_symbols)
    print(("Bloomberg hits : ", hits))



    f = BloombergFundametals()
    i = 0

    tickers_list = []
    for tickers_chunk in _chunks(tickers, size_of_chunks):
        i = i +1
        tickers_list.append(tickers_chunk)

    print(("number of chunks: %d" %i))
    i = 0
    for tickers_chunk in _chunks(tickers,size_of_chunks):
        print(i)
        print((10*"--"))
        print(tickers_chunk)
        print((10 * "--"))

        hits = len(tickers_chunk) * len(fundametal_symbols)
        print(("Bloomberg hits : ", hits))
        if i == chunk_to_work_on:
            data = f.get_bloomberg_Fundametals(tickers_chunk, fundametal_symbols)
            f.store_bloomberg_Fundamentals(data)


        print((10 * "--"))
        i += 1



    # data = f.get_bloomberg_Fundametals(tickers, fundametal_symbols)

    # f.store_bloomberg_Fundamentals(data)

####################################################### part 7 ###########################################################
# get fundamental data for each ticker available

from pycaishen.user_programs.Bloomberg.Bloomberg_get_dvd import BloombergDVD
def get_dvd():
    # storage = PycaishenStorage("arctic")

    from pycaishen.user_programs.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol

    tickers = bloomberg_all_symbol(with_commodity=False,with_currency=False)


    # other method to get only equity
    # import re
    #
    # equity = [x for x in tickers if re.search("Equity" ,x)]
    # print len(equity)


    from pycaishen.user_programs.utilities import _chunks

    batch_size_per_download = 100
    progress = 0
    for tickers_chunk in _chunks(tickers, batch_size_per_download):
        progress = progress + len(tickers_chunk)
        Metadata = BloombergDVD(tickers_chunk)

        meta = Metadata.get_data()
        Metadata.storeTickersData(meta)
        print(("progress: %d over %d" % (progress, len(tickers))))


####################################################### part 8 ###########################################################
# get minute data for each ticker available
from pycaishen.user_programs.Bloomberg.Bloomberg_minute import BloombergIntraday

import datetime
yesterday = datetime.datetime.today() +datetime.timedelta(-1)
yesterday = yesterday.strftime("%d %m %Y")

def get_minute_data(end_date = yesterday, adjusted = True):

    from pycaishen.user_programs.Bloomberg.Bloomberg_all_symbols import bloomberg_all_symbol

    symbols = bloomberg_all_symbol(with_commodity=False)


    Minutedata = BloombergIntraday()
    n = 10
    for symbols in _chunks(symbols,n) :
        print("working on : ")
        print(symbols)
        if adjusted == False :
            data = Minutedata.get_bloomberg_data(bloomberg_symbols_tickers=symbols,finish_date=end_date,adjusted=adjusted)
            # data = EOD.get_bloomberg_EOD(bloomberg_symbols_tickers=symbols)
            lib = Configurer.LIB_BLOOMBERG_MINUTE_NOT_ADJUSTED
            Minutedata.store_bloomberg_data(data,True,lib)
        else:
            data = Minutedata.get_bloomberg_data(bloomberg_symbols_tickers=symbols, finish_date=end_date)
            # data = EOD.get_bloomberg_EOD(bloomberg_symbols_tickers=symbols)
            Minutedata.get_bloomberg_data(data)
            lib = Configurer.LIB_BLOOMBERG_MINUTE_ADJUSTED
            Minutedata.store_bloomberg_data(data, True, lib)


if __name__ == '__main__':

    # 0 correspond to full metadata building
    update = 1

    # get indexes composition
    if update == 1 or update == 0:
        get_composition()
    # get open figi composition
    if update == 2 or update == 0 :
        get_open_figi_data()
    # get metadata from bloomberg
    if update == 3 or update == 0 :
        get_tickers_metadata()
    # get fundamental fields for bloomberg tickers
    if update == 4 or update == 0 :
        set_bloomberg_fundamental_fields()

    # get latest EOD data for all bloomberg tickers
    if update == 5:
        get_EOD_data()

    # get latest EOD data for all bloomberg tickers
    if update == 55:
        get_EOD_data(adjusted=False)

    # get latest fundamental data for all bloomberg ticker
    if update == 6 :
        get_fundamental_data(size_of_chunks=800,chunk_to_work_on=2)

    if update == 7 :
        get_dvd()

    if update == 8:
        get_minute_data()

    if update == 88:
        get_minute_data(adjusted=False)

    # IndexComposition = BloomIndexComposition()
    # indexes=["MOSENEW Index"]
    # from datetime import datetime
    # start_date = datetime.today() + timedelta(-1)
    #
    # # since 2012
    # end_date = datetime(2015, 7, 1, 0, 0, 0, 0)
    # data = IndexComposition.get_composition_date(indexes, start_date, end_date,30)
    #
    # print type(data)
    # import pandas as pd
    # writer = pd.ExcelWriter('output.xlsx',engine='xlsxwriter')
    #
    # for d in data:
    #     print type(d)
    #     # print d
    #     d[["MOSENEW Index.Percent Weight"]]=d[["MOSENEW Index.Percent Weight"]].apply(pd.to_numeric)
    #     d.sort(["MOSENEW Index.Percent Weight"],inplace= True,ascending=False)
    #     print d["MOSENEW Index.Date"][0]
    #     d.to_excel(writer,d["MOSENEW Index.Date"][0].strftime("%Y-%m-%d"))
    #
    # writer.save()


