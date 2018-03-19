import os
"""
this file will hold the settings used by the different modules of pycaishen
"""



class configurer(object):
    """
    abstract class of configuration files
    """
    # Default Pycaishen application name: 'pycaishen'
    APPLICATION_NAME = 'pycaishen'

    VALID_DATASOURCE = ['ats', 'bloomberg', 'dukascopy', 'fred', 'gain', 'google', 'quandl', 'yahoo','reuters']


    ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/') + "/"
    TEMP_FOLDER = ROOT_FOLDER + "temp"

    DEFAULT_MARKET_DATA_REQUEST = "SimpleMarketDataRequest"


class Bloomberg_configuration(configurer):
    # Bloomberg settings
    BBG_SERVER_ADDRESS = "localhost"       # needs changing if you use Bloomberg Server API
    BBG_SERVER_PORT = 8194

class Dukascopy_configuration(configurer):
    # Dukascopy settings
    DUKASCOPY_BASE_URL = "http://www.dukascopy.com/datafeed/"
    DUKASCOPY_WRITE_TEMP_TICK_DISK = False

class Twitter_configuration(configurer):
    # Twitter settings (you need to set these up on Twitter)
    APP_KEY = "6id4R0SkliS68hSYR1utxADaO"
    APP_SECRET = "6vjUHdbhdh2Ub9ojRbhhcCh7bRZRNHuyuKstx4z89EHZKI4JnD"
    OAUTH_TOKEN = "155004161-VdjOReEDvQ9G4KQXBbs6NhEjpWrm6lg0y8Lj6lO9"
    OAUTH_TOKEN_SECRET = "6EdojxNEILAc6wNPo0xWhuFt77rvNf9fTE2XDyEPnDxaL"




class Market_Request_configuration(configurer):
    """
    this file will hold the settings used and need for market requests and the concrete class to use

    """


    VALID_FREQUENCIES = [ "" ,'tick', 'second', 'minute', 'intraday', 'hourly', 'daily', 'weekly', 'monthly', 'quarterly',
                         'annually']



class IO_configuration(configurer):
    """
    the class holds the settings used and need for IO class
    """
    VALID_IO = ["arctic"]
    IO_VERBOSE_MODE = True

class IO_Arctic_configuration(IO_configuration):
    HOST = '127.0.0.1'
    # HOST = '127.0.0.1:57017'

class Logging_configuration(configurer):
    # log config file
    ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.abspath(__file__))).replace('\\', '/') + "/"
    LOGGING_CONF_FILE = ROOT_FOLDER + "util/logging.conf"

class DataSource_configuration(configurer):
    """
    the class holds the settings used in datasource class
    """

class DataSourceUniformizer_configuration(configurer):
    """
    the class holds the settings used in datasource uniformizer class
    """

class Quandl_configuration(configurer):
    """
       the class holds the settings used for Quandl datasource
    """
    QUANDL_API_KEY = "tETRuReNpNs82YcXHCWR"

    QUANDL_NBR_ATTENTS = 3


class Reuters_configuration(configurer):
    """
    the class holds the settings used for Reuters Eikon datasource
    """

    REUTERS_APP_ID="91638EB11FA37EE4A437F698"


class intraday_configuration(configurer):

    VALID_TRADE_SIDE = ['trade', 'bid', 'ask']

class datasource_options(configurer):

    VALID_BLOOMBERG_DATASOURCEOPTIONS = ['override','parameter']

class datasources_tickers_and_fields_limitation_per_request(configurer):

    BLOOMBERG_FIELDS_LIMITATION = 24
    BLOOMBERG_TICKERS_LIMITATION = 500

    REUTERS_FIELDS_LIMITATION = 500
    REUTERES_TICKERS_LIMITATION = 500