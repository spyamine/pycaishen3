"""
Hypothesis: If the 10-day simple moving average (short SMA) of a security is higher than its 30-day simple moving average (long SMA), the price of the security will drop. Conversely, if its short SMA is lower than its long SMA, the price will go up. This is referred to as mean reversion.
"""

from zipline.api import attach_pipeline, pipeline_output # from quantopian.algorithm
from zipline.pipeline import Pipeline
# from zipline.pipeline.data.builtin import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume
# from zipline.pipeline.filters.morningstar import Q500US
from zipline.api import schedule_function, date_rules, time_rules,\
    record,order_target_percent

import logging as log
import pyfolio as pf

def initialize(context):
    """
    initialize() is called once at the start of the program. Any one-time
    startup logic goes here.
    """

    # An assortment of securities from different sectors:
    # MSFT, UNH, CTAS, JNS, COG
    context.security_list = STOCKS

    # Rebalance every Monday (or the first trading day if it's a holiday)
    # at market open.
    schedule_function(rebalance,
                      date_rules.week_start(days_offset=0),
                      time_rules.market_open())

    # Record variables at the end of each day.
    schedule_function(record_vars,
                      date_rules.every_day(),
                      time_rules.market_close())


def compute_weights(context, data):
    """
    Compute weights for each security that we want to order.
    """

    # Get the 30-day price history for each security in our list.
    hist = data.history(context.security_list, 'price', 30, '1d')

    # Create 10-day and 30-day trailing windows.
    prices_10 = hist[-10:]
    prices_30 = hist

    # 10-day and 30-day simple moving average (SMA)
    sma_10 = prices_10.mean()
    sma_30 = prices_30.mean()

    # Weights are based on the relative difference between the short and long SMAs
    raw_weights = (sma_30 - sma_10) / sma_30
    raw_weights = (sma_10 - sma_30) / sma_10

    # Normalize our weights
    normalized_weights = raw_weights / raw_weights.abs().sum()

    # Determine and log our long and short positions.
    short_secs = normalized_weights.index[normalized_weights < 0]
    long_secs = normalized_weights.index[normalized_weights > 0]

    log.info("This week's longs: " + ", ".join([long_.symbol for long_ in long_secs]))
    log.info("This week's shorts: " + ", ".join([short_.symbol for short_ in short_secs]))

    # Return our normalized weights. These will be used when placing orders later.
    return normalized_weights


def rebalance(context, data):
    """
    This function is called according to our schedule_function settings and calls
    order_target_percent() on every security in weights.
    """

    # Calculate our target weights.
    weights = compute_weights(context, data)

    # Place orders for each of our securities.
    for security in context.security_list:
        if data.can_trade(security):
            order_target_percent(security, weights[security])


def record_vars(context, data):
    """
    This function is called at the end of each day and plots our leverage as well
    as the number of long and short positions we are holding.
    """

    # Check how many long and short positions we have.
    longs = shorts = 0
    for position in list(context.portfolio.positions.values()):
        if position.amount > 0:
            longs += 1
        elif position.amount < 0:
            shorts += 1

    # Record our variables.
    record(leverage=context.account.leverage, long_count=longs, short_count=shorts)

if __name__ == '__main__':


    def get_index_tickers(index_ticker):
        from pycaishen.pycaishenstorage import PycaishenStorage

        storage = PycaishenStorage("arctic")
        from pycaishen.user_programs.user_programs_settings import Configurer

        lib = Configurer.LIB_INDEX_COMPOSITION
        print((storage.list_symbols(lib)))
        return list(set(storage.read(index_ticker, lib)["Symbol"].tolist()))

    STOCKS = get_index_tickers("MOSENEW Index")

    print(STOCKS)

    STOCKS = STOCKS[10:30]
    from datetime import datetime
    start = datetime(2008, 1, 1, 0, 0, 0, 0)
    end = datetime(2015, 1, 30, 0, 0, 0, 0)
    from pycaishen.user_programs.Zipline.zipline_EOD_data_from_Bloomberg import Bloomberg_Zipline_EOD_Data

    B_data = Bloomberg_Zipline_EOD_Data(STOCKS, "Bloomberg.EOD")

    data = B_data.load_daily_bars(start, end)
    # if ticker not available it will be removed
    STOCKS = B_data.tickers

    from zipline.algorithm import TradingAlgorithm
    # Create and run the algorithm.
    algo = TradingAlgorithm( initialize=initialize)
    # olmar.set_benchmark("BCE MC Equity")
    results = algo.run(data)

    print(results)

    pyfolioAnalyse = True

    if pyfolioAnalyse:

        returns, positions, transactions, gross_lev = pf.utils.extract_rets_pos_txn_from_zipline(results)

        pf.plot_drawdown_periods(returns, top=5).set_xlabel('Date')

        pf.create_full_tear_sheet(returns, positions=positions, transactions=transactions,
                                  gross_lev=gross_lev, live_start_date='2013-10-22', round_trips=True)

