#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
this will hold the main research function I need for the research environment
'''
from pycaishen.user_programs.user_programs_settings import Configurer

from .pycaishenstorage import PycaishenStorage

import pandas as pd

COLUMNS_FORWARD_BACKWARD_FILL_METHOD = ['mkt_cap', 'high', 'close', 'low', 'open']
COLUMNS_FILL_ZERO = ['turnover', 'volume']

def __missing_data_treatment_fillf_backfill(dataframe, column_to_treat=[]):

    """

    :param dataframe:
    :param column:
    :return:
    """

    # first treatment is fill forward
    columns = dataframe.columns.tolist()
    for column in column_to_treat:
        try:
            dataframe[column] = dataframe[column].fillna(method="ffill")
            # second treatment is fill backward
            dataframe[column] = dataframe[column].fillna(method="backfill")
        except:
            print(("%s not in the data" % column))

    return dataframe

def __missing_data_treatment_zero(dataframe, columns_to_treat=[]):
    """

    :param dataframe:
    :param columns_to_treat:
    :return:
    """
    columns = dataframe.columns.tolist()
    for column in columns_to_treat:
        try:
            dataframe[column] = dataframe[column].fillna(value=0)
        except:
            print(("%s not in the data" % column))

    return dataframe



def _treat_dataframe_list_for_missing_data(data_frame_list, index_range,
                                           columns_forward_backward_fill_method=
                                           COLUMNS_FORWARD_BACKWARD_FILL_METHOD,
                                           columns_fill_zero=COLUMNS_FILL_ZERO):
    """

    :param data_frame_list:
    :param index_range:
    :param columns_forward_backward_fill_method:
    :param columns_fill_zero:
    :return:
    """
    debug = False
    if debug:
        i = 0
    data_frame_list_treated = []
    for dataframe in data_frame_list:
        if dataframe.empty == False:
            # reindex to incorporate all trading days
            # TODO Add support for benchmark reindexing
            if debug:
                print(i)
                i += 1

            # print "before %d" %len(dataframe)
            dataframe = dataframe.reindex(index=index_range)
            __missing_data_treatment_fillf_backfill(dataframe, columns_forward_backward_fill_method)
            __missing_data_treatment_zero(dataframe, columns_fill_zero)
            # print "after %d " % len(dataframe)

            data_frame_list_treated.append(dataframe)
    return data_frame_list_treated


def _get_data(symbols,start_date,end_date ,library_name,source = "arctic"):


    storage = PycaishenStorage(source)


    data_frame_list = []
    tickers_not_available = []
    for ticker in symbols:
        # get the dataframe containing the data we need
        data = storage.read(ticker, library_name, start_date, end_date)
        # drop the rows that are empty
        data = data.dropna(how='all')
        if data.empty == True:
            tickers_not_available.append(ticker)
        else:
            data_frame_list.append(data)



    all_tickers = symbols


    # remove tickers not available
    remaining_tickers = [x for x in all_tickers if x not in tickers_not_available]



    return data_frame_list

def _remove_tickers_name_from_dataframe_list(data_frame_list):

    # removing tickers name from the headers of each dataframe to prepare for a merger
    for data_frame in data_frame_list:
        original_columns = data_frame.columns
        # remove tickers from the columns
        columns = [x.split(".")[1] for x in original_columns]
        # build a header as a dictionary
        header = dict(list(zip(original_columns, columns)))
        # rename the dataframe
        data_frame.rename(columns=header, inplace=True)

    return data_frame_list

def _get_available_tickers(data_frame_list):
    # removing tickers name from the headers of each dataframe to prepare for a merger
    final_tickers =[]
    for data_frame in data_frame_list:
        original_columns = data_frame.columns
        # remove tickers from the columns
        tickers = [x.split(".")[0] for x in original_columns]
        # remove duplicates
        tickers = list(set(tickers))
        final_tickers= final_tickers + tickers



    return final_tickers


def get_pricing(symbols,fields="",start_date='2013-01-03',end_date='2014-01-03',frequency='daily',returned_symbols=[],source ="bloomberg"):
    """
    	get_pricing(symbols, start_date='2013-01-03', end_date='2014-01-03', symbol_reference_date=None, frequency='daily', fields=None, handle_missing='raise')
    	Load a table of historical trade data.
    	Parameters:	• symbols (Object (or iterable of objects) convertible to Asset) – Valid input types are Asset, Integral, or basestring. In the case that the passed objects are strings, they are interpreted as ticker symbols and resolved relative to the date specified by symbol_reference_date.
    		• start_date (str or pd.Timestamp, optional) – String or Timestamp representing a start date for the returned data. Defaults to ‘2013-01-03’.
    		• end_date (str or pd.Timestamp, optional) – String or Timestamp representing an end date for the returned data. Defaults to ‘2014-01-03’.
    		• symbol_reference_date (str or pd.Timestamp, optional) – String or Timestamp representing a date used to resolve symbols that have been held by multiple companies. Defaults to the current time.
    		• frequency ({‘daily’, ‘minute’}, optional) – Resolution of the data to be returned.
    		• fields (str or list, optional) – String or list drawn from {‘price’, ‘open_price’, ‘high’, ‘low’, ‘close_price’, ‘volume’}. Default behavior is to return all fields.
    		• handle_missing ({‘raise’, ‘log’, ‘ignore’}, optional) – String specifying how to handle unmatched securities. Defaults to ‘raise’.
    	Returns:	pandas Panel/DataFrame/Series – The pricing data that was requested. See note below.

    """

    if source == "bloomberg" and frequency == "daily":
        library_name = Configurer.LIB_BLOOMBERG_EOD_ADJUSTED
    elif source =="bloomberg" and frequency == "minute":
        library_name = Configurer.LIB_BLOOMBERG_MINUTE_ADJUSTED
    else:
        library_name = source
    #convert string to symbols
    if type(symbols) is str:
        symbols = [symbols]
    # print type(fields)
    if type(fields) is str :
        fields = [fields]

    dataframe_list = _get_data(symbols,start_date,end_date,library_name)

    # I should get the final tickers I have data for
    available_symbols=_get_available_tickers(dataframe_list)

    for _ in available_symbols:
        returned_symbols.append(_)

    # to detect unavailable stocks
    print("data no available for the following: ")
    print((list(set(symbols).difference(available_symbols))))

     # I should remove tickers name from the fields

    dataframe_list =_remove_tickers_name_from_dataframe_list(dataframe_list)

    # treat the data
    business_day_range = pd.date_range(start_date, end_date)
    dataframe_list = _treat_dataframe_list_for_missing_data(dataframe_list, business_day_range)

    # I should keep only the fields I need then pack the information
    new_dataframe_list = []

    debug= True

    if fields != [""]:
        if debug:

            print((len(available_symbols)))
        i= 0
        for dataframe in dataframe_list:
            # remove the unwanted data
            if debug:
                print((available_symbols[i]))
                i+=1

            dataframe = dataframe[fields]

            new_dataframe_list.append(dataframe)


        dataframe_list = new_dataframe_list

    #depending on the size of symbols and fields there is different format returns
    if (len(fields)>1 and len(symbols)>1) or (fields==[""] and len(symbols)>1) :

        # add "price" to dataframe for generic call


        for dataframe in dataframe_list:
            # remove the unwanted data
            try:

                dataframe["price"] = dataframe["close"]
                new_dataframe_list.append(dataframe)
            except:
                print("error")

        dataframe_list = new_dataframe_list

        data = dict(list(zip(available_symbols, dataframe_list)))
        panel = pd.Panel.from_dict(data)
        panel.dropna(how="all", inplace=True, axis=1)
        return panel

    if len(symbols)>1 and len(fields)==1:
        # columns of dataframe are the symbols
        i = 0

        for dataframe in dataframe_list:
            # renaming the series to hold their tickers name
            dataframe.columns= [available_symbols[i]]
            i = i + 1

        # concatenating the differents pandas series
        dataframe = pd.concat(dataframe_list, join='outer', axis=1)
        # dataframe.dropna(how="all",inplace=True)
        return dataframe

    if len(symbols) == 1 and len(fields) > 1:
        # columns of dataframe are fields
        # concatenating the differents pandas series
        dataframe = pd.concat(dataframe_list, join='outer', axis=1)
        return dataframe

    if len(symbols)==1 and len(fields)==1:
        # return series
        # print type(dataframe_list[0].iloc[0,:])
        return dataframe_list[0].T.iloc[0,:]

def get_fundamentals(symbols,fields="",start_date='2013-01-03',end_date='2014-01-03',frequency='daily',source ="bloomberg"):
    """
    	get_fundamentals(query, base_date, range_specifier=None, filter_ordered_nulls=None)
    	Load a table of historical fundamentals data.
    	Parameters:	• query (SQLAlchemy Query object) – An SQLAlchemy Query representing the fundamentals data desired. Full documentation of the available fields for use in the query function can be found at http://quantopian.com/help/fundamentals
    		• base_date (str in the format “YYYY-MM-DD”) – Represents the date on which data is to be queried. This simulates the backtester making the same query on this date.
    		• range_specifier (str, optional) – String in the format {number}{One of ‘m’, ‘d’, ‘y’, ‘w’, ‘q’}. Represents the interval at which to query data. For example, a base_date of “2014-01-01” with a range_specifier of “4y” will return 4 data values at yearly intervals, from 2014-01-01 going backwards.
    		• filter_ordered_nulls (bool, optional) – When True, if you are sorting the query results via anorder_by method, any row with a NULL value in the sorted column will be filtered out. Setting to False overrides this behavior and provides you with rows with a NULL value for the sorted column.
    	Returns:	pandas.Panel – A Pandas panel containing the requested fundamentals data.
    	Notes
    	The query argument must be built from attributes of the fundamentals namespace returned by init_fundamentals().
    	When querying for a time series, the dates in the major axis of the returned panel are algo dates. This data matches the results of the backtester’s get_fundamentals function being called on each of those dates. This is the best data known to the backtester “before trading starts.”
    	Querying of quarterly data is still under development and may sometimes return inaccurate values.

    """

    if source == "bloomberg" :
        library_name = Configurer.LIB_BLOOMBERG_FUNDAMENTAL_DATA

    else:
        library_name = source
    # convert string to symbols
    if type(symbols) is str:
        symbols = [symbols]
    # print type(fields)
    if type(fields) is str:
        fields = [fields]

    dataframe_list = _get_data(symbols, start_date, end_date, library_name)

    # I should get the final tickers I have data for
    available_symbols = _get_available_tickers(dataframe_list)
    # I should remove tickers name from the fields

    dataframe_list = _remove_tickers_name_from_dataframe_list(dataframe_list)

    # I should keep only the fields I need then pack the information
    new_dataframe_list = []
    if fields != "":
        for dataframe in dataframe_list:
            # remove the unwanted data
            dataframe = dataframe[fields]
            new_dataframe_list.append(dataframe)

    dataframe_list = new_dataframe_list

    # depending on the size of symbols and fields there is different format returns

    data = dict(list(zip(available_symbols, dataframe_list)))
    panel = pd.Panel.from_dict(data)
    panel.dropna(how="all",inplace=True,axis=1)

    return panel

def symbols(symbols,symbol_reference_date=None,handle_missing='log'):
    """
        symbols(symbols, symbol_reference_date=None, handle_missing='log')
        Convert a string or a list of strings into Asset objects.
        Parameters:	• symbols (String or iterable of strings.) – Passed strings are interpreted as ticker symbols and resolved relative to the date specified by symbol_reference_date.
            • symbol_reference_date (str or pd.Timestamp, optional) – String or Timestamp representing a date used to resolve symbols that have been held by multiple companies. Defaults to the current time.
            • handle_missing ({‘raise’, ‘log’, ‘ignore’}, optional) – String specifying how to handle unmatched securities. Defaults to ‘log’.
        Returns:	list of Asset objects – The symbols that were requested.

    """
    raise NotImplementedError

def local_csv(path,symbol_column=None,date_column=None,use_date_column_as_index=True,timezone='UTC',symbol_reference_date=None,**read_csv_kwargs):


    """
        local_csv(path, symbol_column=None, date_column=None, use_date_column_as_index=True, timezone='UTC', symbol_reference_date=None, **read_csv_kwargs)
        Load a CSV from the /data directory.
        Parameters:	• path (str) – Path of file to load, relative to /data.
            • symbol_column (string, optional) – Column containing strings to convert to Asset objects.
            • date_column (str, optional) – Column to parse as Datetime. Ignored if parse_dates is passed as an additional keyword argument.
            • use_date_column_as_index (bool, optional) – If True and date_column is supplied, set it as the frame index.
            • timezone (str or pytz.timezone object, optional) – Interpret date_column as this timezone.
            • read_csv_kwargs (optional) – Extra parameters to forward to pandas.read_csv.
        Returns:	pandas.DataFrame – DataFrame with data from the loaded file.

    """
    raise NotImplementedError

def all_equity_symbols(source = "bloomberg"):

    if source == "bloomberg":

        bloomberg_references = [Configurer.LIB_INDEX_COMPOSITION,Configurer.LIB_BLOOMBERG_OPEN_FIGI]

        print(("working on those tables %s" % bloomberg_references))

        storage = PycaishenStorage("arctic")

        all_symbols =[]

        for lib in bloomberg_references:
            print(("working on %s" % lib))
            symbols = storage.list_symbols(lib)
            for symbol in symbols:
                data = storage.read(symbol,lib)
                data = data["Symbol"].tolist()
                all_symbols = all_symbols + data

        all_symbols = list(set(all_symbols))
        return all_symbols

def index_composition(index_name):
    lib = Configurer.LIB_INDEX_COMPOSITION
    storage = PycaishenStorage("arctic")
    return storage.read(index_name,lib)


if __name__ == '__main__':

    # symbols =["BCE MC Equity","ADH MC Equity","WAA MC Equity","COMI EY Equity","PHAR EY Equity"]
    #
    # start_date = '2013-01-03'
    # end_date = '2017-01-28'
    #
    # fields = ""
    #
    # panel = get_pricing(symbols,fields= fields,start_date=start_date,end_date=end_date)
    #
    # data = panel
    # print data
    # for key in data.minor_axis:
    #     print key
        # print data.minor_xs(key)

    # print panel["ADH MC Equity"]
    #
    # fields = "close"
    # symbols = "ADH MC Equity"
    #
    #
    # dataframe = get_pricing(symbols, fields=fields, start_date=start_date, end_date=end_date)
    #
    #
    # # dataframe.ffill(axis=0,inplace=True )
    # print dataframe
    #
    # fields = ["close","open","high","low"]
    #
    # dataframe = get_pricing("ADH MC Equity", fields=fields, start_date=start_date, end_date=end_date)
    #
    # print dataframe

    # fields = ["close", "open", "high", "low"]
    #
    # storage = PycaishenStorage("arctic")
    #
    # print storage.list_libraries()
    #
    # print storage.list_symbols("Bloomberg.Fundamentals")
    #
    # print storage.read("ADH MC Equity","Bloomberg.Fundamentals").columns
    #
    # fields = ["OPER_INC_GROWTH","NET_INCOME"]
    #
    # panel = get_fundamentals("ADH MC Equity", fields=fields, start_date=start_date, end_date=end_date)
    #
    # print panel["ADH MC Equity"]

    # print len(all_equity_symbols())

    tickers = list(set(index_composition("MOSENEW Index")["Symbol"].tolist()))
    print((len(tickers)))

    field = "mkt_cap"

    available =[]
    data = get_pricing(tickers,field,returned_symbols=available)
    print(data)
    # for key in data.minor_axis:
    #     print key
    #     # print data.minor_xs(key)
    # print len(available)
    #
    # print len(data.items)
    #


