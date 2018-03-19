from pycaishen.datasources.concretedatavendor.datavendorbbg import *

from pycaishen.datasources.concretedatavendor.BBG.lowleveltemplate import BBGLowLevelTemplate

from pycaishen.datasources.datasourcesoptions import OptionsBBG

from pycaishen.util.loggermanager import LoggerManager

from collections import defaultdict
import pandas

class BBGLowLevelHistorical(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelHistorical, self).__init__() # commented becasuse not workng


        self.logger = LoggerManager().getLogger(__name__)
        self._options = []

    def combine_slices(self, data_frame, data_frame_slice):
        if (data_frame_slice.columns.get_level_values(1).values[0]
            not in data_frame.columns.get_level_values(1).values):

            return data_frame.join(data_frame_slice, how="outer")

        return data_frame

    # populate options for Bloomberg request for asset daily request
    def fill_options(self, market_data_request):

        self._options = OptionsBBG()

        self._options.security = market_data_request.data_source_tickers
        self._options.fields = market_data_request.data_source_fields

        if hasattr(market_data_request,"start_date"):
            self._options.startDateTime = market_data_request.start_date

        if hasattr(market_data_request, "finish_date"):
            self._options.endDateTime = market_data_request.finish_date



        if hasattr(market_data_request,"freq"):
            self._options.freq = market_data_request.freq

        if hasattr(market_data_request, "datasource_options"):
            if market_data_request.datasource_options != None:
                self._options.datasource_options = market_data_request.datasource_options

        return self._options

    def process_message(self, msg):
        # Process received events
        ticker = msg.getElement('securityData').getElement('security').getValue()
        fieldData = msg.getElement('securityData').getElement('fieldData')

        # SLOW loop (careful, not all the fields will be returned every time
        # hence need to include the field name in the tuple)
        data = defaultdict(dict)

        for i in range(fieldData.numValues()):
            for j in range(1, fieldData.getValue(i).numElements()):
                data[(str(fieldData.getValue(i).getElement(j).name()), ticker)][fieldData.getValue(i).getElement(0).getValue()] \
                    = fieldData.getValue(i).getElement(j).getValue()

        data_frame = pandas.DataFrame(data)

        # if obsolete ticker could return no values
        if (not(data_frame.empty)):
            # data_frame.columns = pandas.MultiIndex.from_tuples(data, names=['field', 'ticker'])
            data_frame.index = pandas.to_datetime(data_frame.index)
            self.logger.info("Read: " + ticker + ' ' + str(data_frame.index[0]) + ' - ' + str(data_frame.index[-1]))
        else:
            return None

        return data_frame


    # create request for data
    def send_bar_request(self, session, eventQueue):

        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest("HistoricalDataRequest")
        # adding options to the request
        request = self.add_options_to_request(self._options, request)

        self.logger.info("Sending Bloomberg Daily Request:" + str(request))
        session.sendRequest(request)

    def add_options_to_request(self, options, request):
        """

        :param options: options
        :param request: bloomberg request
        :return: request with the options parameters added
        """
        if hasattr(options, "startDateTime"):
            if options.startDateTime != None and options.startDateTime != "":
                request.set("startDate", options.startDateTime.strftime('%Y%m%d'))
        if hasattr(options, "endDateTime"):

            if options.endDateTime != None and options.endDateTime != "":
                request.set("endDate", options.endDateTime.strftime('%Y%m%d'))

        if hasattr(options, "freq"):
            if options.freq != None and options.freq != "":
                request.set("periodicitySelection", str(options.freq).upper())

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

        # # only one security/eventType per request
        for field in options.fields:
            request.getElement("fields").appendValue(field)

        for security in options.security:
            request.getElement("securities").appendValue(security)

        return request
