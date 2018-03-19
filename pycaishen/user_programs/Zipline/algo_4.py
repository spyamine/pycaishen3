from zipline.api import attach_pipeline, pipeline_output # from quantopian.algorithm
from zipline.pipeline import Pipeline
# from zipline.pipeline.data.builtin import USEquityPricing
from zipline.pipeline.factors import AverageDollarVolume, SimpleMovingAverage
# from zipline.pipeline.filters.morningstar import Q500US
from zipline.api import schedule_function, date_rules, time_rules,order_target_percent, record
# from zipline.pipeline.data import USEquityPricing
# from zipline.pipeline.filters.filter import


###########################################################################################


"""
Dataset representing OHLCV data.
"""
from zipline.utils.numpy_utils import float64_dtype

from zipline.pipeline.data.dataset import Column, DataSet



class USEquityPricingAmine(DataSet):
    """
    Dataset representing daily trading prices and volumes.
    """
    open = Column(float64_dtype)
    high = Column(float64_dtype)
    low = Column(float64_dtype)
    close = Column(float64_dtype)
    volume = Column(float64_dtype)

###########################################################################################


def initialize(context):
    # Schedule our rebalance function to run at the start of each week.
    schedule_function(my_rebalance, date_rules.week_start(), time_rules.market_open(hours=1))

    # Record variables at the end of each day.
    schedule_function(my_record_vars, date_rules.every_day(), time_rules.market_close())

    # Create our pipeline and attach it to our algorithm.
    my_pipe = make_pipeline()

    attach_pipeline(my_pipe, 'my_pipeline')

def make_pipeline():
    """
    Create our pipeline.
    """

    # Base universe set to the Q1500US.
    # base_universe = Q1500US()

    # 10-day close price average.
    mean_10 = SimpleMovingAverage(inputs=[USEquityPricingAmine.close], window_length=10) #, mask=base_universe)

    # 30-day close price average.
    mean_30 = SimpleMovingAverage(inputs=[USEquityPricingAmine.close], window_length=30) #, mask=base_universe)

    percent_difference = (mean_10 - mean_30) / mean_30

    # Filter to select securities to short.
    shorts = percent_difference.top(25)

    # Filter to select securities to long.
    longs = percent_difference.bottom(25)

    # Filter for all securities that we want to trade.
    securities_to_trade = (shorts | longs)

    print((Pipeline(
        columns={
            'longs': longs,
            'shorts': shorts
        },
        screen=(securities_to_trade),
    )))
    return Pipeline(
        columns={
            'longs': longs,
            'shorts': shorts
        },
        screen=(securities_to_trade),
    )


def my_compute_weights(context):
    """
    Compute ordering weights.
    """
    # Compute even target weights for our long positions and short positions.
    long_weight = 0.5 / len(context.longs)
    short_weight = -0.5 / len(context.shorts)

    return long_weight, short_weight

def before_trading_start(context, data):
    # Gets our pipeline output every day.
    context.output = pipeline_output('my_pipeline')

    # Go long in securities for which the 'longs' value is True.
    context.longs = context.output[context.output['longs']].index.tolist()

    # Go short in securities for which the 'shorts' value is True.
    context.shorts = context.output[context.output['shorts']].index.tolist()

    context.long_weight, context.short_weight = my_compute_weights(context)

def my_rebalance(context, data):
    """
    Rebalance weekly.
    """
    for security in context.portfolio.positions:
        if security not in context.longs and security not in context.shorts and data.can_trade(security):
            order_target_percent(security, 0)

    for security in context.longs:
        if data.can_trade(security):
            order_target_percent(security, context.long_weight)

    for security in context.shorts:
        if data.can_trade(security):
            order_target_percent(security, context.short_weight)

def my_record_vars(context, data):
    """
    Record variables at the end of each day.
    """
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

    STOCKS = get_index_tickers("MOSENEW Index")[40:50]
    print(STOCKS)

    from datetime import datetime
    # STOCKS = ["BCE MC Equity", "ATW MC Equity","BCP MC Equity","ADH MC Equity"]
    start = datetime(2008, 1, 1, 0, 0, 0, 0)
    end = datetime(2015, 1, 30, 0, 0, 0, 0)

    from pycaishen.user_programs.Zipline.zipline_EOD_data_from_Bloomberg import Bloomberg_Zipline_EOD_Data
    B_data = Bloomberg_Zipline_EOD_Data(STOCKS, "Bloomberg.EOD")

    data = B_data.load_daily_bars(start, end)
    #if ticker not available it will be removed
    STOCKS = B_data.tickers


    from zipline.algorithm import TradingAlgorithm

    # Create and run the algorithm.
    algo = TradingAlgorithm(initialize=initialize,before_trading_start=before_trading_start)
    # olmar.set_benchmark("BCE MC Equity")
    results = algo.run(data)

    print(results)

    pyfolioAnalyse = True

    if pyfolioAnalyse:
        import pyfolio as pf
        returns, positions, transactions, gross_lev = pf.utils.extract_rets_pos_txn_from_zipline(results)

        pf.plot_drawdown_periods(returns, top=5).set_xlabel('Date')

        pf.create_full_tear_sheet(returns, positions=positions, transactions=transactions,
                                  gross_lev=gross_lev, live_start_date='2013-10-22', round_trips=True)