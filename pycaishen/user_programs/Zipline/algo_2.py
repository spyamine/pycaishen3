
import pytz
from datetime import datetime
from zipline.utils.factory import load_bars_from_yahoo
# Load data manually from Yahoo! finance
start = datetime(2011, 1, 1, 0, 0, 0, 0, pytz.utc).date()
end = datetime(2016, 1, 10, 0, 0, 0, 0, pytz.utc).date()
print("getting data ...")

stocks = ["AAPL","SPY"]

def load_bars(stocks,start,end):
    return load_bars_from_yahoo(stocks=stocks, start=start, end=end)

data = load_bars(stocks,start,end)

print(data)

# from pycaishen.examples.BloombergEOD_zipline import Zipline_EOD_Bloomberg
# stocks= ["BCE MC Equity","ATW MC Equity"]
# start = datetime(2014, 1, 1, 0, 0, 0, 0)
# end = datetime(2015, 1, 1, 0, 0, 0, 0)
# zbol = Zipline_EOD_Bloomberg(stocks,"Bloomberg.EOD")
# print zbol.load_daily_bars(start, end)
#
print("data ok ...")
from zipline.algorithm import TradingAlgorithm


"""Dual Moving Average Crossover algorithm.
This algorithm buys apple once its short moving average crosses
its long moving average (indicating upwards momentum) and sells
its shares once the averages cross again (indicating downwards
momentum).
"""


print("Backtesting .... ")
from zipline.api import order_target, record, symbol
# import zipline
# symbol_stocks =[]
# for stock in stocks:
#     print type(stock)
#     zipline.api.symbol(stock)
#     symbol_stocks.append(stock)

def initialize(context):
    context.sym = [symbol('AAPL'),symbol("SPY")]
    context.i = 0


def handle_data(context, data):
    # Skip first 300 days to get full windows
    context.i += 1
    if context.i < 300:
        return

    # Compute averages
    # history() has to be called with the same params
    # from above and returns a pandas dataframe.
    short_mavg = data.history(context.sym, 'price', 100, '1d').mean()
    long_mavg = data.history(context.sym, 'price', 300, '1d').mean()

    # Trading logic
    if short_mavg[0] > long_mavg[0]:
        # order_target orders as many shares as needed to
        # achieve the desired number of shares.
        order_target(context.sym[0], 100)
    elif short_mavg[0] < long_mavg[0]:
        order_target(context.sym[0], 0)

    # Save values for later inspection
    record(AAPL=data.current(context.sym[0], "price"),
           short_mavg=short_mavg[0],
           long_mavg=long_mavg[0])


# Note: this function can be removed if running
# this algorithm on quantopian.com
def analyze(context=None, results=None):
    import matplotlib as mil
    mil.use('TkAgg')

    import matplotlib.pyplot as plt
    import logbook
    logbook.StderrHandler().push_application()
    log = logbook.Logger('Algorithm')

    fig = plt.figure()
    ax1 = fig.add_subplot(211)
    results.portfolio_value.plot(ax=ax1)
    ax1.set_ylabel('Portfolio value (USD)')

    ax2 = fig.add_subplot(212)
    ax2.set_ylabel('Price (USD)')

    # If data has been record()ed, then plot it.
    # Otherwise, log the fact that no data has been recorded.
    if ('AAPL' in results and 'short_mavg' in results and
            'long_mavg' in results):
        results['AAPL'].plot(ax=ax2)
        results[['short_mavg', 'long_mavg']].plot(ax=ax2)

        trans = results.ix[[t != [] for t in results.transactions]]
        buys = trans.ix[[t[0]['amount'] > 0 for t in
                         trans.transactions]]
        sells = trans.ix[
            [t[0]['amount'] < 0 for t in trans.transactions]]
        ax2.plot(buys.index, results.short_mavg.ix[buys.index],
                 '^', markersize=10, color='m')
        ax2.plot(sells.index, results.short_mavg.ix[sells.index],
                 'v', markersize=10, color='k')
        plt.legend(loc=0)
    else:
        msg = 'AAPL, short_mavg & long_mavg data not captured using record().'
        ax2.annotate(msg, xy=(0.1, 0.5))
        log.info(msg)

    plt.show()



algo = TradingAlgorithm(initialize=initialize, handle_data=handle_data,analyze=analyze)
results = algo.run(data)

print("printing last results dataframe: ")
print((results.tail()))

pyfolioAnalyse = True

if pyfolioAnalyse:
    import pyfolio as pf

    returns, positions, transactions, gross_lev = pf.utils.extract_rets_pos_txn_from_zipline(results)


    pf.plot_drawdown_periods(returns, top=5).set_xlabel('Date')


    pf.create_full_tear_sheet(returns, positions=positions, transactions=transactions,
                              gross_lev=gross_lev, live_start_date='2014-10-22', round_trips=True)
