from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.pycaishendata import PycaishenData

from pycaishen.user_programs.user_programs_settings import Configurer

import datetime

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

class ReutersEOD():

    def __init__(self):
        pass

    def get_EOD_tickers(self):
        storage = PycaishenStorage('arctic')
        tickers = storage.list_symbols(Configurer.REUTERS_TICKERS_LIB)
        return tickers

    def getting_last_date(self):
        import datetime
        from datetime import timedelta

        one_day = timedelta(days=1)
        end_date = str(datetime.date.today() - one_day)
        return end_date

    def get_EOD(self,tickers , start_date = "2000-01-01", end_date = ""):

        # setting the end date to yesterday date if nothing provided
        if end_date == "":
            end_date = self.getting_last_date()

        # number of elements per chunk
        n_chunks = 10
        # initialization of the storage object
        storage = PycaishenStorage('arctic')
        # looping through the whole chunk per tickers
        for tickers_chunk in chunks(tickers,n_chunks):
            # getting the data from eikon
            print("getting the data from eikon...")
            fields = ["CLOSE","OPEN","LOW","HIGH","VOLUME","COUNT"]
            try:
                df = ek.get_timeseries(tickers_chunk,fields ,start_date=start_date,end_date=end_date)
            except:
                print("no data")
            # getting the tickers our of the returned dataframe

            try:
                returned_tickers = df.columns.levels[0].tolist()
                # storing the data looping by returned tickers
                for ticker in returned_tickers:
                    try:
                        print(("storing %s " % ticker))
                        storage.write(ticker,df[ticker].dropna(how="all"),Configurer.REUTERS_EOD_LIB)
                    except:
                        print(("error storing %s " % ticker))
            except:
                print("Error")






if __name__ == '__main__':
    r = ReutersEOD()
    # tickers = r.get_EOD_tickers()
    # r.get_EOD(tickers)

    storage = PycaishenStorage('arctic')
    tickers = storage.list_symbols(Configurer.REUTERS_EOD_LIB)
    print((len(tickers)))
    # for t in tickers:
    #     print storage.read(t,Configurer.REUTERS_EOD_LIB).head()



