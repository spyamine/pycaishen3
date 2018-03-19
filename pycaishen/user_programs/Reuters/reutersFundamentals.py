from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.pycaishendata import PycaishenData

from pycaishen.user_programs.user_programs_settings import Configurer

import datetime
import pandas as pd

import eikon as ek
ek_id = "91638EB11FA37EE4A437F698"
ek.set_app_id(ek_id)

def chunks(list, n):
    """Yield successive n-sized chunks from l."""

    if n > 1:
        for i in range(0, len(list), n):
            yield list[i:i + n]
    elif n == 1:
        for i in range(0, len(list)):
            yield list[i:i + 1]

class ReutersFundamental():

    def __init__(self):
        pass

    def get_tickers(self):
        # initiating storage
        storage = PycaishenStorage('arctic')
        # getting all available tickers in the library
        tickers = storage.list_symbols(Configurer.REUTERS_TICKERS_LIB)
        df = []
        for ticker in tickers:
            df.append(storage.read(ticker,Configurer.REUTERS_TICKERS_LIB))
        # concatenate the list of dataframes into one dataframe to take the items i need
        df = pd.concat(df)

        tickers = {}
        # classifiying the tickers per company type to get the correct fundamentals per company type
        tickers["Banks"] = df[df["GICS Industry Name"]== "Banks"].Identifier.tolist()
        tickers["Insurance"] = df[df["GICS Industry Name"] == "Insurance"].Identifier.tolist()
        tickers["Industrial"] = df[(df["GICS Industry Name"]!="Insurance") & (df["GICS Industry Name"]!="Banks")].Identifier.tolist()

        return tickers

    def get_fields(self):
        """

        :return: a dictionary containing the fields needed for every company type
        """
        storage = PycaishenStorage('arctic')
        companiesType = storage.list_symbols(Configurer.REUTERS_FIELDS_LIB)
        fields = {}
        for comp in companiesType:
            fields[comp]=storage.read(comp,Configurer.REUTERS_FIELDS_LIB)['reuters_ticker'].tolist()
            print(comp)
        return fields

    def get_Fundamentals(self, tickers, fields, years = 20, frequency = 'FY'):

        for comp_type in list(tickers.keys()):
            print(("working on %s type " % comp_type))

            # number of elements per chunk
            n_chunks = 10
            # initialization of the storage object
            storage = PycaishenStorage('arctic')
            # looping through the whole chunk per tickers
            for tickers_chunk in chunks(tickers[comp_type], n_chunks):
                # getting the data from eikon
                # tickers_chunk = tickers_chunk[:3]
                # print len(tickers_chunk)
                fields_s = fields[comp_type]
                # in order to get the dates
                fields_s = ['TR.Revenue.date'] + fields_s
                # print fields_s

                try:
                    print("getting the data from eikon...")
                    df ,er = ek.get_data(tickers_chunk, fields_s ,{'SDate': 0, 'EDate': -years, 'FRQ': frequency})
                except:
                    print("no data")
                # getting the tickers our of the returned dataframe
                # print df
                try:
                    #getting the returned tickers
                    returned_tickers = df.Instrument.unique().tolist()
                    print(returned_tickers)
                    # storing the data looping by returned tickers
                    for ticker in returned_tickers:
                        try:
                            print(("storing %s " % ticker))
                            # adding frequency to the dataframe
                            dff = df[(df.Instrument == ticker)].dropna(how="all")
                            dff["Frequency"] = frequency
                            # print dff.head()
                            storage.write(ticker, dff, Configurer.REUTERS_FUNDAMENTAL_DATA)
                        except:
                            print(("error storing %s " % ticker))
                except:
                    print("Error")


if __name__ == '__main__':
    # r = ReutersFundamental()
    # tickers = r.get_tickers()
    # fields = r.get_fields()
    # print tickers.keys()
    # print fields.keys()
    # r.get_Fundamentals(tickers,fields)

    storage = PycaishenStorage('arctic')
    tickers = storage.list_symbols(Configurer.REUTERS_FUNDAMENTAL_DATA)
    print((len(tickers)))
    # for t in tickers:
    #     print storage.read(t,Configurer.REUTERS_FUNDAMENTAL_DATA).head()