import pandas
from pycaishen.util.loggermanager import LoggerManager
class AbstractReference(object):
    """
    Abstract class for reference building
    """
    def __init__(self):
        self.logger = LoggerManager().getLogger(__name__)

    def _csv_to_dataframe(self,csv_file,separator=None):
        try:
            if separator is None:
                return pandas.read_csv(csv_file, dtype=object)
            else:
                return pandas.read_csv(csv_file,sep= separator, dtype=object)
        except:
            return None

    def _excel_to_dataframe(self,excel_file,sheetname):

        try:
            return pandas.read_excel(excel_file, sheetname)
        except:
            return None



class Tickers(AbstractReference):

    def __init__(self):
        super(Tickers,self).__init__()
        self.datasource_tickers=[]
        self.tickers=[]

    def setTickers(self,datasource_tickers,tickers = []):

        if type(datasource_tickers) =="str":
            datasource_tickers = [datasource_tickers]

        self.datasource_tickers = datasource_tickers

        if type(tickers) =="str":
            tickers = [tickers]

        if tickers != []:
            self.tickers = tickers
        else:
            self.tickers = []

    def fromCSV(self,csv_file,separator = None):
        dataframe = self._csv_to_dataframe(csv_file,separator)
        # print dataframe
        # self.logger.warning("Problem with the csv file: " + str(csv_file))
        datasource_tickers = []
        tickers= []
        try:
            columns = list(dataframe.columns)
            # print columns
            if "datasource_tickers" in columns:
                datasource_tickers = list(dataframe["datasource_tickers"])
            if "tickers" in columns:
                tickers = list(dataframe["tickers"])
            self.setTickers(datasource_tickers,tickers)

        except:
            self.logger.warning("Problem with the csv file: "+str(csv_file) )


    def fromExcel(self, excel_file, sheetname):
        dataframe = self._excel_to_dataframe(excel_file, sheetname)
        datasource_tickers = []
        tickers = []
        try:
            columns = list(dataframe.columns)
            # print columns
            if "datasource_tickers" in columns:
                datasource_tickers = list(dataframe["datasource_tickers"])
            if "tickers" in columns:
                tickers = list(dataframe["tickers"])
            self.setTickers(datasource_tickers, tickers)

        except:
            self.logger.warning("Problem with the Excel file: " + str(excel_file)+ " "+ str(sheetname))


class Fields(AbstractReference):

    def __init__(self):
        super(Fields, self).__init__()
        self.datasource_fields = []
        self.fields = []

    def setFields(self, datasource_fields, fields=[]):

        if type(datasource_fields) == "str":
            datasource_fields = [datasource_fields]

        self.datasource_fields = datasource_fields

        if type(fields) == "str":
            fields = [fields]

        if fields != []:
            self.fields = fields
        else :
            self.fields = []

    def fromCSV(self,csv_file,separator = None):
        dataframe = self._csv_to_dataframe(csv_file, separator)

        datasource_fields = []
        fields = []
        try:
            columns = list(dataframe.columns)
            # print columns
            if "datasource_fields" in columns:
                datasource_fields = list(dataframe["datasource_fields"])
            if "fields" in columns:
                fields = list(dataframe["fields"])
            self.setFields(datasource_fields, fields)

        except:
            self.logger.warning("Problem with the csv file: " + str(csv_file))

    def fromExcel(self,excel_file,sheetname):
        dataframe = self._excel_to_dataframe(excel_file, sheetname)
        datasource_fields = []
        fields = []
        try:
            columns = list(dataframe.columns)

            if "datasource_fields" in columns:
                datasource_fields = list(dataframe["datasource_fields"])
            if "fieldsthe" in columns:
                fields = list(dataframe["fields"])
            self.setFields(datasource_fields, fields)

        except:
            self.logger.warning("Problem with the Excel file: " + str(excel_file) + " " + str(sheetname))


if __name__ == '__main__':


    from pycaishen.util.settings import configurer as setting
    root =  setting.ROOT_FOLDER
    current_folder = root + "reference/"
    print(current_folder)
    ticker = Tickers()
    # datasource_ticker=["ADH MC Equity","BCP MC Equity"]
    # tickers_simple = ["ADH","BCP"]
    # ticker.setTickers(datasource_ticker,tickers_simple)
    # print ticker.datasource_tickers
    # print ticker.tickers
    # ticker.setTickers(datasource_ticker)
    # print ticker.datasource_tickers
    # print ticker.tickers

    ticker_csv = current_folder +"tickers.csv"
    tickers_excel = current_folder +"tickers.xlsx"
    ticker.fromCSV(ticker_csv,";")
    print((ticker.datasource_tickers))
    print((ticker.tickers))

    ticker.setTickers([])
    sheetname = "Feuil1"
    ticker.fromExcel(tickers_excel, sheetname)
    print((ticker.datasource_tickers))
    print((ticker.tickers))

    print(("fields testing "+ 10* "**"))

    fields = Fields()
    ticker_csv = current_folder + "fields.csv"
    tickers_excel = current_folder + "fields.xlsx"
    fields.fromCSV(ticker_csv, ";")
    print((fields.datasource_fields))
    print((fields.fields))

    fields.setFields([])
    sheetname = "Feuil1"
    fields.fromExcel(tickers_excel, sheetname)
    print((fields.datasource_fields))
    print((fields.fields))
