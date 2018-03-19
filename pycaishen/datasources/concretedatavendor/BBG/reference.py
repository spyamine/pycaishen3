from pycaishen.datasources.concretedatavendor.datavendorbbg import *
from pycaishen.datasources.concretedatavendor.BBG.lowleveltemplate import BBGLowLevelTemplate
from pycaishen.datasources.datasourcesoptions import OptionsBBG
from pycaishen.util.loggermanager import LoggerManager

import re
import pandas
import collections

try:
    import blpapi   # obtainable from Bloomberg website
except: pass


class BBGLowLevelRef(BBGLowLevelTemplate):

    def __init__(self):
        super(BBGLowLevelRef, self).__init__()

        self.logger = LoggerManager().getLogger(__name__)
        self._options = []

    # populate options for Bloomberg request for asset intraday request
    def fill_options(self, market_data_request):
        self._options = OptionsBBG()

        self._options.security = market_data_request.data_source_tickers
        if hasattr(market_data_request,"start_date") :
            if market_data_request.start_date != None and market_data_request.start_date != "":
                self._options.startDateTime = market_data_request.start_date
        if hasattr(market_data_request,"finish_date"):
            if market_data_request.finish_date != None and market_data_request.finish_date != "":
                self._options.endDateTime = market_data_request.finish_date

        self._options.fields = market_data_request.data_source_fields

        if hasattr(market_data_request, "datasource_options"):
            if market_data_request.datasource_options != None and market_data_request.datasource_options != "":
                self._options.datasource_options = market_data_request.datasource_options

        return self._options

    def process_message_original(self, msg):
        data = collections.defaultdict(dict)

        # process received events
        securityDataArray = msg.getElement('securityData')

        index = 0

        for securityData in list(securityDataArray.values()):
            ticker = securityData.getElementAsString("security")
            fieldData = securityData.getElement("fieldData")

            for field in fieldData.elements():
                if not field.isValid():
                    field_name = "%s" % field.name()

                    self.logger.error(field_name + " is NULL")
                elif field.isArray():
                    # iterate over complex data returns.
                    field_name = "%s" % field.name()

                    # here is the problem with reference data
                    for i, row in enumerate(field.values()):
                        data[(field_name, ticker)][index] = re.findall(r'"(.*?)"', "%s" % row)[0]

                        index = index + 1
                # else:
                    # vals.append(re.findall(r'"(.*?)"', "%s" % row)[0])
                    # print("%s = %s" % (field.name(), field.getValueAsString()))

            fieldExceptionArray = securityData.getElement("fieldExceptions")

            for fieldException in list(fieldExceptionArray.values()):
                errorInfo = fieldException.getElement("errorInfo")
                print((errorInfo.getElementAsString("category"), ":", \
                    fieldException.getElementAsString("fieldId")))

        data_frame = pandas.DataFrame(data)

        # if obsolete ticker could return no values
        if (not(data_frame.empty)):
            data_frame.columns = pandas.MultiIndex.from_tuples(data, names=['field', 'ticker'])
            self.logger.info("Reading: " + ticker + ' ' + str(data_frame.index[0]) + ' - ' + str(data_frame.index[-1]))
        else:
            return None

        return data_frame

    def process_message(self, msg):

        from pandas import DataFrame

        # FIELD_ID = blpapi.Name("fieldId")
        SECURITY_DATA = blpapi.Name("securityData")
        SECURITY = blpapi.Name("security")
        FIELD_DATA = blpapi.Name("fieldData")
        # FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
        # ERROR_INFO = blpapi.Name("errorInfo")

        data_return = DataFrame()

        securityDataArray = msg.getElement(SECURITY_DATA)

        for securityData in list(securityDataArray.values()):
            security = securityData.getElementAsString(SECURITY)
            fieldData = securityData.getElement(FIELD_DATA)
            dictt = self._dict_from_element(fieldData)


            fields = list(dictt.keys())
            for field in fields:
                try:

                    contents = dictt[field]
                    data = DataFrame(contents)

                    if data_return.empty :
                        data_return = data
                    else :
                        #TODO pay attention to this and checxk if it works well
                        data_return.append(data)
                except:
                    # made this in order that bdp function like works
                    data_return = DataFrame.from_dict([dictt])
                    break

            fieldExceptionArray = securityData.getElement("fieldExceptions")

            for fieldException in list(fieldExceptionArray.values()):
                errorInfo = fieldException.getElement("errorInfo")
                print((errorInfo.getElementAsString("category"), ":", \
                      fieldException.getElementAsString("fieldId")))

        data_frame = data_return

        return data_frame

    def _dict_from_element(self,element):
        '''
        Used for e.g. dividends
        '''
        try:
            return element.getValueAsString()

        except:
            if element.numValues() > 1:
                results = []
                for i in range(0, element.numValues()):
                    subelement = element.getValue(i)
                    name = str(subelement.name())
                    results.append(self._dict_from_element(subelement))
            else:
                results = {}
                for j in range(0, element.numElements()):
                    subelement = element.getElement(j)
                    name = str(subelement.name())
                    results[name] = self._dict_from_element(subelement)
            return results

    def combine_slices(self, data_frame, data_frame_slice):
        if (data_frame_slice.columns.get_level_values(1).values[0]
            not in data_frame.columns.get_level_values(1).values):

            return data_frame.join(data_frame_slice, how="outer")

        return data_frame

    # create request for data
    def send_bar_request(self, session, eventQueue):
        refDataService = session.getService("//blp/refdata")
        request = refDataService.createRequest('ReferenceDataRequest')

        request = self.add_options_to_request(self._options,request)
        self.logger.info("Sending Bloomberg Ref Request:" + str(request))

        session.sendRequest(request)

    def add_options_to_request(self, options, request):

        if hasattr(options, "startDateTime"):
            if options.startDateTime != None and options.startDateTime != "":
                self.add_override(request, 'START_DT', options.startDateTime.strftime('%Y%m%d'))
        if hasattr(options, "endDateTime"):
            if options.endDateTime != None and options.endDateTime != "":
                self.add_override(request, 'END_DT', options.endDateTime.strftime('%Y%m%d'))

        # only one security/eventType per request
        for field in options.fields:
            request.getElement("fields").appendValue(field)

        for security in options.security:
            request.getElement("securities").appendValue(security)

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

        return request

    def _add_options_to_request(self, options, request):

        if hasattr(options,"startDateTime"):
            self.add_override(request, 'START_DT', self._options.startDateTime.strftime('%Y%m%d'))
        if hasattr(options, "endDateTime"):
            self.add_override(request, 'END_DT', self._options.endDateTime.strftime('%Y%m%d'))

        # only one security/eventType per request
        for field in options.fields:
            request.getElement("fields").appendValue(field)

        for security in options.security:
            request.getElement("securities").appendValue(security)

        # self.add_override(request, 'TIME_ZONE_OVERRIDE', 23)    # force GMT time

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