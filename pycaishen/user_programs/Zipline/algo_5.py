
"""
This is a template algorithm on Quantopian for you to adapt and fill in.
"""
# from quantopian.algorithm import attach_pipeline, pipeline_output
# from quantopian.pipeline import Pipeline
# from quantopian.pipeline.data.builtin import USEquityPricing
# from quantopian.pipeline.factors import AverageDollarVolume
# from quantopian.pipeline.filters.morningstar import Q500US

from zipline.api import attach_pipeline, pipeline_output # from quantopian.algorithm
from zipline.pipeline import Pipeline
# from zipline.pipeline.data.builtin import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume
# from zipline.pipeline.filters.morningstar import Q500US
from zipline.api import schedule_function, date_rules, time_rules, order, record,symbol

def initialize(context):
    """
    Called once at the start of the algorithm.
    """
    # Rebalance every day, 1 hour after market open.
    context.stocks = [symbol(x) for x in stocks]

def make_pipeline():
    """
    A function to create our dynamic stock selector (pipeline). Documentation on
    pipeline can be found here: https://www.quantopian.com/help#pipeline-title
    """

    pass

def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    pass

def my_assign_weights(context, data):
    """
    Assign weights to securities that we want to order.
    """
    pass

def my_rebalance(context ,data):
    """
    Execute orders according to our schedule_function() timing.
    """
    pass

def my_record_vars(context, data):
    """
    Plot variables at the end of each day.
    """
    pass

def handle_data(context ,data):
    """
    Called every minute.
    """
    for stock in context.stocks:
        order(stock,10)
    # record(AAPL= data.current(symbol('AAPL'),'close'))
    pass

def analyze(perf):
    import platform
    platform = platform.system()
    if platform == "Darwin":
        import matplotlib as mil
        mil.use('TkAgg')

    import matplotlib.pyplot as plt

    ax1 = plt.subplot(211)
    perf.algo_volatility.plot(ax=ax1)
    ax1.set_ylabel('algo_volatility ')
    ax2 = plt.subplot(212, sharex=ax1)
    perf.algorithm_period_return.plot(ax=ax2)
    ax2.set_ylabel('algorithm_period_return')
    plt.show()

yahoo_data = False
if yahoo_data :
    # get the data
    import pytz
    from datetime import datetime
    from zipline.utils.factory import load_bars_from_yahoo

    start = datetime(2011, 1, 1, 0, 0, 0, 0, pytz.utc).date()
    end = datetime(2016, 1, 10, 0, 0, 0, 0, pytz.utc).date()
    print("getting data ...")

    stocks = ["AAPL","SPY"]


    data = load_bars_from_yahoo(stocks=stocks, start=start, end=end)
else:
    def get_index_tickers(index_ticker):
        from pycaishen.pycaishenstorage import PycaishenStorage

        storage = PycaishenStorage("arctic")
        from pycaishen.user_programs.user_programs_settings import Configurer

        lib = Configurer.LIB_INDEX_COMPOSITION
        print((storage.list_symbols(lib)))
        return list(set(storage.read(index_ticker, lib)["Symbol"].tolist()))

    stocks = get_index_tickers("MOSENEW Index")[40:50]


    from datetime import datetime
    stocks = ["BCE MC Equity", "ATW MC Equity","BCP MC Equity","ADH MC Equity"]
    start = datetime(2008, 1, 1, 0, 0, 0, 0)
    end = datetime(2015, 1, 30, 0, 0, 0, 0)

    from pycaishen.user_programs.Zipline.zipline_EOD_data_from_Bloomberg import Bloomberg_Zipline_EOD_Data
    B_data = Bloomberg_Zipline_EOD_Data(stocks, "Bloomberg.EOD")

    data = B_data.load_daily_bars(start, end)
    #if ticker not available it will be removed
    stocks = B_data.tickers


from zipline.algorithm import TradingAlgorithm

# Create and run the algorithm.
algo = TradingAlgorithm(initialize=initialize,handle_data=handle_data)

results = algo.run(data)


print((results.tail()))

analyze(results)

pyfolioAnalyse = False

if pyfolioAnalyse:
    import platform
    platform = platform.system()
    if platform == "Darwin":
        import matplotlib as mil
        mil.use('TkAgg')

    import matplotlib.pyplot as plt

    print(("working on: %s system " %platform))
    print("working on pyfolio analysis ")

    import pyfolio as pf
    returns, positions, transactions, gross_lev = pf.utils.extract_rets_pos_txn_from_zipline(results)

    # pf.plot_drawdown_periods(returns, top=5).set_xlabel('Date')


    pf.create_full_tear_sheet(returns, positions=positions, transactions=transactions,
                              gross_lev=gross_lev, live_start_date='2014-10-22', round_trips=True)