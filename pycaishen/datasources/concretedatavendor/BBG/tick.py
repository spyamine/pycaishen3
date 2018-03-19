from pycaishen.datasources.concretedatavendor.datavendorbbg import *
from pycaishen.datasources.concretedatavendor.BBG.lowleveltemplate import BBGLowLevelTemplate
from pycaishen.datasources.datasourcesoptions import OptionsBBG
from pycaishen.util.loggermanager import LoggerManager

import pandas
from operator import itemgetter
try:
    import blpapi   # obtainable from Bloomberg website
except: pass


class BBGLowLevelTick(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelTick, self).__init__()

        self.logger = LoggerManager().getLogger(__name__)

        # constants
        self.TICK_DATA = blpapi.Name("tickData")
        self.COND_CODE = blpapi.Name("conditionCodes")
        self.TICK_SIZE = blpapi.Name("size")
        self.TIME = blpapi.Name("time")
        self.TYPE = blpapi.Name("type")
        self.VALUE = blpapi.Name("value")
        self.RESPONSE_ERROR = blpapi.Name("responseError")
        self.CATEGORY = blpapi.Name("category")
        self.MESSAGE = blpapi.Name("message")
        self.SESSION_TERMINATED = blpapi.Name("SessionTerminated")

    def combine_slices(self, data_frame, data_frame_slice):
        return data_frame.append(data_frame_slice)

    # populate options for Bloomberg request for asset intraday request
    def fill_options(self, market_data_request):
        self._options = OptionsBBG()

        self._options.security = market_data_request.data_source_tickers[0]    # get 1st ticker only!
        # self._options.event = market_data_request.trade_side.upper()
        # self._options.barInterval = market_data_request.freq_mult
        self._options.startDateTime = market_data_request.start_date
        self._options.endDateTime = market_data_request.finish_date
        # self._options.gapFillInitialBar = False

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
        data = msg.getElement(self.TICK_DATA).getElement(self.TICK_DATA)

        self.logger.info("Processing tick data for " + str(self._options.security))
        tuple = []

        data_vals = list(data.values())


        # slightly faster this way (note, we are skipping trade & CC fields)
        tuple = [([item.getElementAsFloat(self.VALUE),
                             item.getElementAsInteger(self.TICK_SIZE)],
                             item.getElementAsDatetime(self.TIME)) for item in data_vals]

        data_table = list(map(itemgetter(0), tuple))
        time_list = list(map(itemgetter(1), tuple))

        try:
            self.logger.info("Dates between " + str(time_list[0]) + " - " + str(time_list[-1]))
        except:
            self.logger.info("No dates retrieved")
            return None

        # create pandas dataframe with the Bloomberg output
        return pandas.DataFrame(data = data_table, index = time_list,
                      columns=['close', 'ticksize'])

    # implement abstract method: create request for data
    def send_bar_request(self, session, eventQueue):
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("IntradayTickRequest")

        request = self.add_options_to_request(self._options,request)

        request.getElement("eventTypes").appendValue("TRADE")
        # request.set("eventTypes", self._options.event)
        request.set("includeConditionCodes", True)

        # self.add_override(request, 'TIME_ZONE_OVERRIDE', 'GMT')



        self.logger.info("Sending Tick Bloomberg Request...")

        session.sendRequest(request)

    def add_options_to_request(self, options, request):
        # only one security/eventType per request
        request.set("security", self._options.security)

        if hasattr(options, "startDateTime"):
            if options.startDateTime != None and options.startDateTime != "":
                request.set("startDateTime", options.startDateTime)
        if hasattr(options, "endDateTime"):
            if options.endDateTime != None and options.endDateTime != "":
                request.set("endDateTime", options.endDateTime)

        if hasattr(options, "datasource_options"):
            if options.datasource_options != None and options.datasource_options != "":

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