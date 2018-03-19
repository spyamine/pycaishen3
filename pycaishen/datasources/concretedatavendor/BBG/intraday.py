from pycaishen.datasources.concretedatavendor.datavendorbbg import *
from pycaishen.datasources.concretedatavendor.BBG.lowleveltemplate import BBGLowLevelTemplate
from pycaishen.util.loggermanager import LoggerManager
from pycaishen.datasources.datasourcesoptions import OptionsBBG


from pycaishen.util.settings import intraday_configuration as settings
import pandas
from operator import itemgetter

try:
    import blpapi   # obtainable from Bloomberg website
except: pass


class BBGLowLevelIntraday(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelIntraday, self).__init__()

        self.logger = LoggerManager().getLogger(__name__)

        # constants
        self.BAR_DATA = blpapi.Name("barData")
        self.BAR_TICK_DATA = blpapi.Name("barTickData")
        self.OPEN = blpapi.Name("open")
        self.HIGH = blpapi.Name("high")
        self.LOW = blpapi.Name("low")
        self.CLOSE = blpapi.Name("close")
        self.VOLUME = blpapi.Name("volume")
        self.NUM_EVENTS = blpapi.Name("numEvents")
        self.TIME = blpapi.Name("time")

    def combine_slices(self, data_frame, data_frame_slice):
        return data_frame.append(data_frame_slice)

    # populate options for Bloomberg request for asset intraday request

    def _compute_freq_mult(self,market_data_request):

        if hasattr(market_data_request, "freq"):
            if market_data_request.freq == "minute":
                return 1
            else: return 1

    def _setting_trade_side(self,market_data_request):
        if hasattr(market_data_request,"trade_side") :
            if market_data_request.trade_side in settings.VALID_TRADE_SIDE:
                return market_data_request.trade_side
        else: return "trade" #default trade side


    def fill_options(self, market_data_request):
        self._options = OptionsBBG()

        self._options.security = market_data_request.data_source_tickers[0]    # get 1st ticker only!

        # setting the trade_side
        trade_side = self._setting_trade_side(market_data_request)
        trade_side = str(trade_side).upper()
        self._options.event = trade_side

        freq_mult = self._compute_freq_mult(market_data_request)

        self._options.barInterval = freq_mult
        self._options.startDateTime = market_data_request.start_date
        self._options.endDateTime = market_data_request.finish_date
        self._options.gapFillInitialBar = False

        if hasattr(self._options.startDateTime, 'microsecond'):
            self._options.startDateTime = self._options.startDateTime.replace(microsecond=0)

        if hasattr(self._options.endDateTime, 'microsecond'):
            self._options.endDateTime = self._options.endDateTime.replace(microsecond=0)

        if hasattr(market_data_request, "datasource_options"):
            if market_data_request.datasource_options != None:
                self._options.datasource_options = market_data_request.datasource_options

        return self._options

    # iterate through Bloomberg output creating a DataFrame output
    # implements abstract method
    def process_message(self, msg):
        data = msg.getElement(self.BAR_DATA).getElement(self.BAR_TICK_DATA)

        self.logger.info("Processing intraday data for " + str(self._options.security))

        data_vals = list(data.values())


        tuple = [([bar.getElementAsFloat(self.OPEN),
                        bar.getElementAsFloat(self.HIGH),
                        bar.getElementAsFloat(self.LOW),
                        bar.getElementAsFloat(self.CLOSE),
                        bar.getElementAsInteger(self.VOLUME),
                        bar.getElementAsInteger(self.NUM_EVENTS)],
                        bar.getElementAsDatetime(self.TIME)) for bar in data_vals]

        data_table = list(map(itemgetter(0), tuple))
        time_list = list(map(itemgetter(1), tuple))

        try:
            self.logger.info("Dates between " + str(time_list[0]) + " - " + str(time_list[-1]))
        except:
            self.logger.info("No dates retrieved")
            return None

        # create pandas dataframe with the Bloomberg output
        return pandas.DataFrame(data = data_table, index = time_list,
                      columns=['open', 'high', 'low', 'close', 'volume', 'events'])

    # implement abstract method: create request for data
    def send_bar_request(self, session, eventQueue):
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("IntradayBarRequest")



        request = self.add_options_to_request(self._options,request)

        self.logger.info("Sending Intraday Bloomberg Request...")

        session.sendRequest(request)

    def add_options_to_request(self, options, request):

        # only one security/eventType per request
        if hasattr(options,"security"):
            request.set("security", options.security)
        if hasattr(options, "event"):
            request.set("eventType", options.event)
        if hasattr(options, "barInterval"):
            request.set("interval", options.barInterval)

        # self.add_override(request, 'TIME_ZONE_OVERRIDE', 'GMT')

        if hasattr(options, "startDateTime"):
            if options.startDateTime != None and options.startDateTime != "":
                request.set("startDateTime", options.startDateTime)

        if hasattr(options, "endDateTime"):
            if options.endDateTime != None and options.endDateTime != "":
                request.set("endDateTime", options.endDateTime)

        if hasattr(options, "gapFillInitialBar"):
            if options.gapFillInitialBar :
                request.append("gapFillInitialBar", True)

        if hasattr(options, "datasource_options"):

            options_type = options.datasource_options.options_type
            options_field = options.datasource_options.options_fields
            options_values = options.datasource_options.options_values

            i = 0
            for type in options_type:
                if type == "override":
                    self.add_override(request, options_field[i], options_values[i])

                elif type == 'parameter':
                    request.set(options_field[i], str(options_values[i]).upper())
                i = i + 1

        return request