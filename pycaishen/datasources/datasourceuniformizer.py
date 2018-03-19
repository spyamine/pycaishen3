from pycaishen.util.settings import DataSourceUniformizer_configuration as settings


class AbstractDataOutputUniformizer(object):
    """
    Abstract class that uniformize the data output from the datasource class
    this class should return a list of dataframes containing data for every ticker in the data
    """


    def uniformize(self):
        raise NotImplementedError



    def _return_tickers_column(self,column):
        """

        :param column: inform of [ticker1.colx,ticker2.col, ....]
        :return: list of unique tickers
        """
        #remove the fields from the column
        col = [item.split(".")[0] for item in column]
        #remove duplicate tickers
        col = list(set(col))
        return col


    def _pandas_outer_join(self, df_list):
        if df_list is None: return None

        # remove any None elements (which can't be joined!)
        df_list = [i for i in df_list if i is not None]

        if len(df_list) == 0: return None
        elif len(df_list) == 1: return df_list[0]

        # df_list = [dd.from_pandas(df) for df in df_list]

        return df_list[0].join(df_list[1:], how="outer")


class GenericDataOutputUniformizer(AbstractDataOutputUniformizer):

    """
    You may use this class to ovewrite uniformizer class function
    """
    def __init__(self):
        pass

    def uniformize(self,list_dataframe):

        global_returned_tickers = []
        uniformized_dataframe_list = []
        #retrieve the unique ticker returned
        for data_frame in list_dataframe:

            if data_frame is not None:
                columns = data_frame.columns

                clean_tickers = self._return_tickers_column(columns)
                #concateneting the tickers
                global_returned_tickers = global_returned_tickers + clean_tickers
                #remove duplicated tickers from global
                global_returned_tickers = list(set(global_returned_tickers))

        # print "global_returned_tickers : "
        # print global_returned_tickers

        for ticker in global_returned_tickers:
            # ticker_dataframe = pd.DataFrame()
            # print "working on : " + ticker
            intermediate_ticker_dataframe_list=[]
            for data_frame in list_dataframe:
                if data_frame is not None:
                    ticker_cols = [col for col in data_frame.columns if ticker in col]

                    # print data_frame[ticker_cols].head(2)
                    intermediate_ticker_dataframe_list.append(data_frame[ticker_cols])
            ticker_dataframe = self._pandas_outer_join(intermediate_ticker_dataframe_list)


            # old method was working
            # ticker_dataframe = ticker_dataframe.dropna()
            ticker_dataframe = ticker_dataframe.dropna(how='all')

            uniformized_dataframe_list.append(ticker_dataframe)

        return uniformized_dataframe_list


class BloombergDataOutputUniformizer(AbstractDataOutputUniformizer):
    """
    You may use this class to ovewrite uniformizer class function
    """
    pass


class ReutersDataOutputUniformizer(AbstractDataOutputUniformizer):

    """
    You may use this class to ovewrite uniformizer class function
    """
    def __init__(self):
        pass

    def uniformize(self,list_dataframe):

        global_returned_tickers = []
        uniformized_dataframe_list = []
        #retrieve the unique ticker returned
        for data_frame in list_dataframe:

            if data_frame is not None:
                columns = data_frame.columns

                clean_tickers = self._return_tickers_column(columns)
                #concateneting the tickers
                global_returned_tickers = global_returned_tickers + clean_tickers
                #remove duplicated tickers from global
                global_returned_tickers = list(set(global_returned_tickers))

        # print "global_returned_tickers : "
        # print global_returned_tickers

        for ticker in global_returned_tickers:
            # ticker_dataframe = pd.DataFrame()
            # print "working on : " + ticker
            intermediate_ticker_dataframe_list=[]
            for data_frame in list_dataframe:
                if data_frame is not None:
                    ticker_cols = [col for col in data_frame.columns if ticker in col]

                    # print data_frame[ticker_cols].head(2)
                    intermediate_ticker_dataframe_list.append(data_frame[ticker_cols])
            ticker_dataframe = self._pandas_outer_join(intermediate_ticker_dataframe_list)


            # old method was working
            # ticker_dataframe = ticker_dataframe.dropna()
            ticker_dataframe = ticker_dataframe.dropna(how='all')

            uniformized_dataframe_list.append(ticker_dataframe)

        return uniformized_dataframe_list

class AbstractDataOutputUniformizerFactory(object):
    """
    Abstract class that will choose the right class to use depending on the concrete datasource
    """

    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class DataOutputUniformizerFactory(AbstractDataOutputUniformizerFactory):
    def __call__(self, datasource_name):
        datasource_name = str(datasource_name).lower()
        if datasource_name in settings.VALID_DATASOURCE:
            if datasource_name == 'reuters':
                return ReutersDataOutputUniformizer
            else:
                return GenericDataOutputUniformizer


if __name__ == '__main__':
    import pandas as pd
    import numpy as np

    columns1 = ["tick1.col1","tick2.col2","tick1.col3"]
    columns2 = ["tick2.col1", "tick1.col2", "tick2.col3"]

    df1 = pd.DataFrame(np.random.randn(50, 3), columns=columns1)
    df2 = pd.DataFrame(np.random.randn(50, 3), columns=columns2)

    print((df1.head(2)))
    print((df2.head(2)))
    df_list = [df1,df2]
    uniformizer = DataOutputUniformizerFactory()
    concreteUniformizer = uniformizer("quandl")
    concreteUniformizer = concreteUniformizer()
    print((concreteUniformizer.uniformize(df_list)))
    # uni = uniformizer.uniformize(df_list)
    #
    # for df in uni:
    #     print df.head(2)

    # # columns1_tickers
    # columns1_tickers = []
    # for item in columns1:
    #     item_treat = item.split(".")[0]
    #     columns1_tickers.append(item_treat)
    # #remove duplicates
    # columns1_tickers = list(set(columns1_tickers))
    #
    # print columns1_tickers
    #
    # col1 = [item.split(".")[0] for item in columns1]
    # col1 = list(set(col1))
    # print col1
    # l = columns1_tickers + col1
    # print l
    # ticker = "tick1"
    # t1 =  [col for col in df1.columns if ticker in col]
    #
    # ticker = "tick2"
    # t2 =  [col for col in df1.columns if ticker in col]
    #
    # print df1[t1]
    # print df1[t2]