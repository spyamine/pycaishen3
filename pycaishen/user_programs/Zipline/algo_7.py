"""
A simple Pipeline algorithm that longs the top 3 stocks by RSI and shorts
the bottom 3 each day.
"""
from six import viewkeys
from zipline.api import (
    attach_pipeline,
    date_rules,
    order_target_percent,
    pipeline_output,
    record,
    schedule_function,
)
from zipline.api import get_environment
from zipline.pipeline import Pipeline
from zipline.pipeline.factors import RSI, SimpleMovingAverage


def make_pipeline():
    rsi = RSI()
    return Pipeline(
        columns={
            'longs': rsi.top(3),
            'shorts': rsi.bottom(3),
        },
    )


def rebalance(context, data):
    # Pipeline data will be a dataframe with boolean columns named 'longs' and
    # 'shorts'.
    pipeline_data = context.pipeline_data
    all_assets = pipeline_data.index

    longs = all_assets[pipeline_data.longs]
    shorts = all_assets[pipeline_data.shorts]
    print(("longs: " + str(longs)))
    record(universe_size=len(all_assets))
    print(("shorts: " + str(shorts)))
    print(("portfolio value: " + str(context.portfolio.portfolio_value)))
    # Build a 2x-leveraged, equal-weight, long-short portfolio.
    one_third = 1.0 / 3.0
    for asset in longs:
        order_target_percent(asset, one_third)

    for asset in shorts:
        order_target_percent(asset, -one_third)

    # Remove any assets that should no longer be in our portfolio.
    portfolio_assets = longs | shorts

    positions = context.portfolio.positions
    for asset in viewkeys(positions) - set(portfolio_assets):
        # This will fail if the asset was removed from our portfolio because it
        # was delisted.
        if data.can_trade(asset):
            order_target_percent(asset, 0)


def initialize(context):
    pipe = Pipeline()
    pipe = attach_pipeline(pipe, name='my_pipeline')

    # Construct Factors.
    sma_10 = SimpleMovingAverage([data[stocks].close],10)
    sma_30 = SimpleMovingAverage([data[stocks].close],10)

    # Construct a Filter.
    prices_under_5 = (sma_10 < 5)

    # Register outputs.
    pipe.add(sma_10, 'sma_10')
    pipe.add(sma_30, 'sma_30')

    # Remove rows for which the Filter returns False.
    pipe.set_screen(prices_under_5)



def handle_data(context, data):
    pass


def before_trading_start(context, data):
    # Access results using the name passed to `attach_pipeline`.
    results = pipeline_output('my_pipeline')
    print((results.head(5)))

    # Store pipeline results for use by the rest of the algorithm.
    context.pipeline_results = results



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

yahoo_data = True
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
    stocks = ["MOSENEW Index", "ATW MC Equity","BCP MC Equity","ADH MC Equity"]
    start = datetime(2008, 1, 1, 0, 0, 0, 0)
    end = datetime(2015, 1, 30, 0, 0, 0, 0)

    from pycaishen.user_programs.Zipline.zipline_EOD_data_from_Bloomberg import Bloomberg_Zipline_EOD_Data
    B_data = Bloomberg_Zipline_EOD_Data(stocks, "Bloomberg.EOD")

    data = B_data.load_daily_bars(start, end)
    #if ticker not available it will be removed
    stocks = B_data.tickers


from zipline.algorithm import TradingAlgorithm

capital_base = 1000000
data_frequency = 'daily'
# start = datetime(2008, 1, 1, 0, 0, 0, 0,pytz.utc).date()
# end = datetime(2015, 1, 30, 0, 0, 0, 0, pytz.utc).date()
# Create and run the algorithm.
# algo = TradingAlgorithm(initialize=initialize,handle_data = handle_data ,before_trading_start=before_trading_start,start = start, end = end)
import pandas as pd
algo = TradingAlgorithm(start = pd.Timestamp('2013-10-07', tz='utc'),end = pd.Timestamp('2013-11-30', tz='utc') ,initialize=initialize,handle_data = handle_data ,before_trading_start=before_trading_start)

results = algo.run(data)


print((results.tail()))

analyze(results)
