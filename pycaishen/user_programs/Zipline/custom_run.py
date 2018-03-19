"""

https://groups.google.com/forum/#!topic/zipline/KHKI1PZx08I

Hi Ed,

I can and would like to help out with this.  I have live traded with IbPy in the past.  
The documentation for DataPortal is pretty good, Blotter could use a little more documentation.

Scott gave some good advice on using those two classes, and then next question is after those get subclassed and the proper methods overridden, where would you plug them in? If you dig into the code Zipline uses to run an algorithm you will find that the heart of it is a TradingAlgorithm, most people will never see this if they are letting Zipline "run" the algorithm for them.  The Zipline run function basically just sets up a TradingAlgorithm and runs it.  (I've included complete code at the bottom of this post showing how to run a backtest on your own, it shows how TradingAlgorithm is set up.)  

TradingAlgorithm is where I would plug in the custom DataPortal, let's call it LiveDataPortal (see line #274 of algorithm.py), you can pass it as an argument.  Now where to plug in the custom Blotter, (let's call it LiveBlotter)?  Guess what?  You also can pass it as an argument to TradingAlgorithm (see line #321 of algorithm.py). 

So now that those questions are answered the real work of getting LiveDataPortal and LiveBlotter to talk to IB would need to happen.  

Like I said I could and would like to help out with this, so think of a project name and we can get started.  
"""



"""
This code is used to demonstrate how to run a Zipline backtest from within code.  (not using the command line tool)
The reason for doing is using Python to drive the running of multiple backtests.

A good reference is the zipline code that actually runs the backtest:
   _run() in zipline/utils/run_algo.py
"""
from zipline import TradingAlgorithm
from zipline.data.data_portal import DataPortal
from zipline.finance.trading import TradingEnvironment
from zipline.utils.factory import create_simulation_parameters
from zipline.utils.calendars import get_calendar
from zipline.pipeline.loaders import USEquityPricingLoader
from zipline.pipeline.data.equity_pricing import USEquityPricing
from zipline.data.bundles.core import load
from zipline.api import symbol, order  # used in handle_data

import os
import re
from time import time
import pandas as pd

CAPITAL_BASE = 1.0e6


def makeTS(date_str):
    """creates a Pandas DT object from a string"""
    return pd.Timestamp(date_str, tz='utc')


def parse_sqlite_connstr(db_URL):
    """parses out the db connection string (needed to make a TradingEnvironment"""
    _, connstr = re.split(r'sqlite:///', str(db_URL), maxsplit=1,)
    return connstr


def make_choose_loader(pl_loader):
    def cl(column):
        if column in USEquityPricing.columns:
            return pipeline_loader
        raise ValueError("No PipelineLoader registered for column %s." % column)
    return cl


if __name__ == '__main__':

    # load the bundle
    bundle_data = load('quantopian-quandl', os.environ, None)
    cal = bundle_data.equity_daily_bar_reader.trading_calendar.all_sessions
    pipeline_loader = USEquityPricingLoader(bundle_data.equity_daily_bar_reader, bundle_data.adjustment_reader)
    choose_loader = make_choose_loader(pipeline_loader)

    env = TradingEnvironment(asset_db_path=parse_sqlite_connstr(bundle_data.asset_finder.engine.url))

    data = DataPortal(
        env.asset_finder, get_calendar("NYSE"),
        first_trading_day=bundle_data.equity_minute_bar_reader.first_trading_day,
        equity_minute_reader=bundle_data.equity_minute_bar_reader,
        equity_daily_reader=bundle_data.equity_daily_bar_reader,
        adjustment_reader=bundle_data.adjustment_reader,
    )

    start = makeTS("2015-11-01"); end = makeTS("2016-11-01")  # this can go anywhere before the TradingAlgorithm

    def initialize(context):
        pass

    def handle_data(context, data):
        order(symbol('AAPL'), 10)

    # the actual running of the backtest happens in the TradingAlgorithm object
    bt_start = time()
    perf = TradingAlgorithm(
        env=env,
        get_pipeline_loader=choose_loader,
        sim_params=create_simulation_parameters(
            start=start,
            end=end,
            capital_base=CAPITAL_BASE,
            data_frequency='daily',
        ),
        **{
            'initialize': initialize,
            'handle_data': handle_data,
            'before_trading_start': None,
            'analyze': None,
        }
    ).run(data, overwrite_sim_params=False,)
    bt_end = time()

    print((perf.columns))
    print((perf['portfolio_value']))

    print(("The backtest took %0.2f seconds to run." % (bt_end - bt_start)))
    print("all done boss")
