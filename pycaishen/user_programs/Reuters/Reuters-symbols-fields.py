from pycaishen.pycaishenstorage import PycaishenStorage
from pycaishen.pycaishendata import PycaishenData

from pycaishen.user_programs.user_programs_settings import Configurer

import datetime

class ReutersMetadata():

    def __init__(self):
        pass

    def chunks(list, n):
        """Yield successive n-sized chunks from l."""

        if n > 1:
            for i in range(0, len(list), n):
                yield list[i:i + n]
        elif n == 1:
            for i in range(0, len(list)):
                yield list[i:i + 1]

    def _get_fields(self,excel_name="reuters tickers.xlsx" , sheet_name ="Summary" ):
        """

        :return: dataframe of all the fields available to have for Reuters

        """
        import pandas as pd

        data_fields = pd.read_excel(excel_name, sheet_name)
        return  data_fields

    def _store_fields(self,data_fields,company_type_column="company-type"):
        storage = PycaishenStorage('arctic')
        types = data_fields["company-type"].unique()
        print(types)
        for t in types:
            fields = data_fields[data_fields["company-type"] == t]
            storage.write(t,fields,Configurer.REUTERS_FIELDS_LIB)

    def set_fields(self):
        df = self._get_fields()
        self._store_fields(df)

    def _get_tickers(self,excel_name="reuters african companies.xlsx", sheet_name ="Current Screen Template"):
        """

        :param excel_name:
        :param sheet_name:
        :return: dataframe containing all the references regarding the african space
        """
        import pandas as pd

        data_tickers = pd.read_excel(excel_name, sheet_name)
        return data_tickers

    def _store_tickers(self,df):
        """
        will loop through each instrument and store the data in the database
        :param df:
        :return: nothing
        """
        storage = PycaishenStorage('arctic')
        tickers_list = df.Identifier.unique().tolist()
        for ticker in tickers_list:
            info= df[(df.Identifier == ticker)]
            storage.write(ticker,info,Configurer.REUTERS_TICKERS_LIB)

    def set_tickers(self):
        df = self._get_tickers()
        self._store_tickers(df)




if __name__ == '__main__':

    meta = ReutersMetadata()
    meta.set_tickers()


