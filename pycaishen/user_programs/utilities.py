import pandas as pd


def remove_tickers_from_dataframe_columns_name(dataframe):
    """

    :param dataframe:
    :return:
    """

    columns = dataframe.columns.tolist()

    columns = [x.split(".",1)[1] for x in columns]

    dataframe.columns = columns

    return dataframe

def get_symbol_from_dataframe(dataframe):
    """

    :param dataframe:
    :return:
    """

    columns = dataframe.columns.tolist()

    columns = [x.split(".", 1)[0] for x in columns]

    return columns[0]

def _chunks(list, n):
    """Yield successive n-sized chunks from l."""

    if n > 1:
        for i in range(0, len(list), n):
            yield list[i:i + n]
    elif n == 1:
        for i in range(0, len(list)):
            yield list[i:i + 1]

def missing_data_treatment_fillf_backfill(dataframe,columns_to_treat=[]):

    """

    :param dataframe:
    :param columns_to_treat:
    :return:
    """

    # first treatment is fill forward
    dataframe[columns_to_treat] = dataframe[columns_to_treat].fillna(method="ffill")

    # second treatment is fill backward
    dataframe[columns_to_treat] = dataframe[columns_to_treat].fillna(method="backfill")

    return dataframe

def missing_data_treatment_zero(dataframe, columns_to_treat=[]):
    """

    :param dataframe:
    :param columns_to_treat:
    :return:
    """
    dataframe[columns_to_treat] = dataframe[columns_to_treat].fillna(value=0)

    return dataframe

def date_range(start_date,end_date,business = True):
    """

    :param start_date:
    :param end_date:
    :return:
    """
    if business:

        return pd.bdate_range(start=start_date, end=end_date)
    else:
        return pd.date_range(start=start_date, end=end_date)

def plot_data(data,symbol):

    import matplotlib as mil
    mil.use('TkAgg')

    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import matplotlib.dates as mdates
    from matplotlib.finance import  candlestick_ohlc

    date = data.index
    close = data.close.values
    open = data.close.values
    high = data.high.values
    low = data.low.values
    volume = data.volume.values

    x = 0
    y = len(open)
    candleAr = []

    while x< y:
        appendLine = mdates.date2num(date[x]),open[x],high[x],low[x], close[x],volume[x]
        candleAr.append(appendLine)
        x+=1



    fig = plt.figure()
    ax1 = plt.subplot2grid((5,4),(0,0),rowspan=4,colspan=4)

    candlestick_ohlc(ax1, candleAr,width=2 ,colorup='g',colordown='r')

    ax1.grid(True)
    ax1.xaxis.set_major_locator(mticker.MaxNLocator(10))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.ylabel("stock price of %s " % symbol)

    ax2 = plt.subplot2grid((5, 4), (4, 0),sharex= ax1 ,rowspan=1, colspan=4)
    ax2.bar(date,volume)
    ax2.axes.yaxis.set_ticklabels([])
    ax2.grid(True)
    plt.ylabel('Volume')

    for label in ax2.xaxis.get_ticklabels():
        label.set_rotation(45)
    plt.show()

def simple_plot(dataframe):

    import matplotlib as mil
    mil.use('TkAgg')

    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates

    columns = dataframe.columns.tolist()
    # fig = plt.figure()


    date = dataframe.index

    # Two subplots, the axes array is 1-d
    f, axarr = plt.subplots(len(columns), sharex=True)


    date_m = []
    for i in range(len(date)):

        date_m.append(mdates.date2num(date[i]))

    col_nbr = 0
    for col in columns:

        x = 0
        y = len(date)
        data = []

        while x < y:
            appendLine = dataframe[col].values[x]
            data.append(appendLine)
            x += 1

        axarr[col_nbr].plot(date_m,data,label="test1")
        axarr[col_nbr].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        col_nbr += 1

        axarr[0].set_title(symbol)
    plt.show()



if __name__ == '__main__':


    from pycaishen.pycaishenstorage import PycaishenStorage

    storage = PycaishenStorage("arctic")

    lib = "Bloomberg.EOD"

    symbols = storage.list_symbols(lib)

    symbols = ["SAM MC Equity", "WAA MC Equity"]
    dataframe_list = []
    for symbol in symbols:
        dataframe_list.append( storage.read(symbol, lib))


    for dataframe in dataframe_list:
        symbol =  get_symbol_from_dataframe(dataframe)
        remove_tickers_from_dataframe_columns_name(dataframe)
        # print dataframe.columns.tolist()
        dt_range = date_range("2013-01-01","2014-01-01")
        print((len(dt_range)))
        dataframe = dataframe.reindex(index = dt_range  )

        columns_forward_backward_fill_method = ['mkt_cap', 'high', 'close', 'low', 'open']

        columns_fill_zero = [ 'volume', 'turnover']
        missing_data_treatment_fillf_backfill(dataframe,columns_forward_backward_fill_method)
        missing_data_treatment_zero(dataframe,columns_fill_zero)

        print((dataframe.head()))
        print(("lenght of dataframe: %d" % dataframe.shape[0]))
        # print dataframe[dataframe.high < dataframe.low]


        # plot_data(dataframe,symbol)

        simple_plot(dataframe)
        # break


