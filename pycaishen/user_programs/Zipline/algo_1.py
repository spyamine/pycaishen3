
import platform
system =  platform.system()
print(("working on: %s system " % system))

if system == "Darwin":
    import matplotlib as mil

    mil.use('TkAgg')

import sys
from datetime import datetime

import logbook
import numpy as np
import pyfolio as pf
from zipline.algorithm import TradingAlgorithm
from zipline.finance import commission



# Zipline trading algorithm
# Taken from zipline.examples.olmar

zipline_logging = logbook.NestedSetup([
    logbook.NullHandler(level=logbook.DEBUG),
    logbook.StreamHandler(sys.stdout, level=logbook.INFO),
    logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
])
zipline_logging.push_application()




# On-Line Portfolio Moving Average Reversion

# More info can be found in the corresponding paper:
# http://icml.cc/2012/papers/168.pdf
def initialize(context, eps=1, window_length=5):
    context.stocks = STOCKS
    context.sids = [context.symbol(symbol) for symbol in context.stocks]
    context.m = len(context.stocks)
    context.price = {}
    context.b_t = np.ones(context.m) / context.m
    context.last_desired_port = np.ones(context.m) / context.m
    context.eps = eps
    context.init = True
    context.days = 0
    context.window_length = window_length

    context.set_commission(commission.PerShare(cost=0.02))


def handle_data(algo, data):
    algo.days += 1
    if algo.days < algo.window_length:
        return

    if algo.init:
        rebalance_portfolio(algo, data, algo.b_t)
        algo.init = False
        return

    m = algo.m

    x_tilde = np.zeros(m)
    b = np.zeros(m)

    # find relative moving average price for each asset
    mavgs = data.history(algo.sids, 'price', algo.window_length, '1d').mean()
    for i, sid in enumerate(algo.sids):
        price = data.current(sid, "price")
        # Relative mean deviation
        x_tilde[i] = mavgs[sid] / price

    ###########################
    # Inside of OLMAR (algo 2)
    x_bar = x_tilde.mean()

    # market relative deviation
    mark_rel_dev = x_tilde - x_bar

    # Expected return with current portfolio
    exp_return = np.dot(algo.b_t, x_tilde)
    weight = algo.eps - exp_return
    variability = (np.linalg.norm(mark_rel_dev)) ** 2

    # test for divide-by-zero case
    if variability == 0.0:
        step_size = 0
    else:
        step_size = max(0, weight / variability)

    b = algo.b_t + step_size * mark_rel_dev
    b_norm = simplex_projection(b)
    np.testing.assert_almost_equal(b_norm.sum(), 1)

    rebalance_portfolio(algo, data, b_norm)

    # update portfolio
    algo.b_t = b_norm


def rebalance_portfolio(algo, data, desired_port):
    # rebalance portfolio
    desired_amount = np.zeros_like(desired_port)
    current_amount = np.zeros_like(desired_port)
    prices = np.zeros_like(desired_port)

    if algo.init:
        positions_value = algo.portfolio.starting_cash
    else:
        positions_value = algo.portfolio.positions_value + \
            algo.portfolio.cash

    for i, sid in enumerate(algo.sids):
        current_amount[i] = algo.portfolio.positions[sid].amount
        prices[i] = data.current(sid, "price")

    desired_amount = np.round(desired_port * positions_value / prices)

    algo.last_desired_port = desired_port
    diff_amount = desired_amount - current_amount

    for i, sid in enumerate(algo.sids):
        algo.order(sid, diff_amount[i])


def simplex_projection(v, b=1):
    """Projection vectors to the simplex domain
    Implemented according to the paper: Efficient projections onto the
    l1-ball for learning in high dimensions, John Duchi, et al. ICML 2008.
    Implementation Time: 2011 June 17 by Bin@libin AT pmail.ntu.edu.sg
    Optimization Problem: min_{w}\| w - v \|_{2}^{2}
    s.t. sum_{i=1}^{m}=z, w_{i}\geq 0
    Input: A vector v \in R^{m}, and a scalar z > 0 (default=1)
    Output: Projection vector w
    :Example:
    >>> proj = simplex_projection([.4 ,.3, -.4, .5])
    >>> print(proj)
    array([ 0.33333333, 0.23333333, 0. , 0.43333333])
    >>> print(proj.sum())
    1.0
    Original matlab implementation: John Duchi (jduchi@cs.berkeley.edu)
    Python-port: Copyright 2013 by Thomas Wiecki (thomas.wiecki@gmail.com).
    """

    v = np.asarray(v)
    p = len(v)

    # Sort v into u in descending order
    v = (v > 0) * v
    u = np.sort(v)[::-1]
    sv = np.cumsum(u)

    rho = np.where(u > (sv - b) / np.arange(1, p + 1))[0][-1]
    theta = np.max([0, (sv[rho] - b) / (rho + 1)])
    w = (v - theta)
    w[w < 0] = 0
    return w

if __name__ == '__main__':

    # STOCKS = ['AMD', 'CERN', 'COST', 'DELL', 'GPS', 'INTC', 'MMM']
    # start = datetime(2004, 1, 1, 0, 0, 0, 0, pytz.utc)
    # end = datetime(2010, 1, 1, 0, 0, 0, 0, pytz.utc)
    #
    # # Load price data from yahoo.
    # data = load_from_yahoo(stocks=STOCKS, indexes={}, start=start, end=end)
    # data = data.dropna()

    # print data

    def get_index_tickers(index_ticker):
        from pycaishen.pycaishenstorage import PycaishenStorage

        storage = PycaishenStorage("arctic")
        from pycaishen.user_programs.user_programs_settings import Configurer

        lib = Configurer.LIB_INDEX_COMPOSITION
        print((storage.list_symbols(lib)))
        return list(set(storage.read(index_ticker, lib)["Symbol"].tolist()))

    STOCKS = get_index_tickers("MOSENEW Index")
    print(STOCKS)


    STOCKS = ["BCE MC Equity", "ATW MC Equity","BCP MC Equity","ADH MC Equity"]
    start = datetime(2014, 1, 1, 0, 0, 0, 0)
    end = datetime(2015, 1, 30, 0, 0, 0, 0)
    # from pycaishen.user_programs.Zipline.zipline_EOD_data_from_Bloomberg import Bloomberg_Zipline_EOD_Data
    # B_data = Bloomberg_Zipline_EOD_Data(STOCKS, "Bloomberg.EOD")
    #
    # data = B_data.load_daily_bars(start, end)
    #
    # print data
    #
    #if ticker not available it will be removed
    # STOCKS = B_data.tickers

    from pycaishen.pycaishenresearch import get_pricing

    data = get_pricing(STOCKS,"",start,end)

    # Create and run the algorithm.
    olmar = TradingAlgorithm(handle_data=handle_data, initialize=initialize)
    # olmar.set_benchmark("BCE MC Equity")

    # olmar.set_benchmark()
    results = olmar.run(data)

    print("printing last results dataframe: ")
    print((results.tail()))

    pyfolioAnalyse = True

    if pyfolioAnalyse:

        returns, positions, transactions, gross_lev = pf.utils.extract_rets_pos_txn_from_zipline(results)


        pf.plot_drawdown_periods(returns, top=5).set_xlabel('Date')


        pf.create_full_tear_sheet(returns, positions=positions, transactions=transactions,
                                  gross_lev=gross_lev, live_start_date='2014-10-22', round_trips=True)

