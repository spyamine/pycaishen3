import abc
import pandas

from pycaishen.util.settings import Bloomberg_configuration as Bloomberg_settings
from pycaishen.util.loggermanager import LoggerManager

try:
    import blpapi   # obtainable from Bloomberg website
except: pass



# gather the common methods for BBGLowLevelIntraday, BBGLowLevelTick and BBGLowLevelDaily
class BBGLowLevelTemplate(object):

    _session = None

    def __init__(self):
        self._data_frame = None

        self.RESPONSE_ERROR = blpapi.Name("responseError")
        self.SESSION_TERMINATED = blpapi.Name("SessionTerminated")
        self.CATEGORY = blpapi.Name("category")
        self.MESSAGE = blpapi.Name("message")
        self.logger = LoggerManager().getLogger(__name__)

        # return

    def load_time_series(self, market_data_request):

        # will use sub classes fill_options method
        options = self.fill_options(market_data_request)

        #if(BBGLowLevelTemplate._session is None):
        session = self.start_bloomberg_session()
        #else:
        #    session = BBGLowLevelTemplate._session

        try:
            # if can't open the session, kill existing one
            # then try reopen (up to 5 times...)
            i = 0

            while i < 5:
                if session is not None:
                    if not session.openService("//blp/refdata"):
                        self.logger.info("Try reopening Bloomberg session... try " + str(i))
                        self.kill_session(session) # need to forcibly kill_session since can't always reopen
                        session = self.start_bloomberg_session()

                        if session is not None:
                            if session.openService("//blp/refdata"): i = 6
                else:
                    self.logger.info("Try opening Bloomberg session... try " + str(i))
                    session = self.start_bloomberg_session()

                i = i + 1

            # give error if still doesn't work after several tries..
            if not session.openService("//blp/refdata"):
                self.logger.error("Failed to open //blp/refdata")

                return

            self.logger.info("Creating request...")
            eventQueue = None # blpapi.EventQueue()

            # create a request
            self.send_bar_request(session, eventQueue)
            self.logger.info("Waiting for data to be returned...")

            # wait for events from session and collect the data
            self.event_loop(session, eventQueue)

        finally:
            # stop the session (will fail if NoneType)
            try:
                session.stop()
            except: pass

        return self._data_frame

    def event_loop(self, session, eventQueue):
        not_done = True

        data_frame = pandas.DataFrame()
        data_frame_slice = None

        while not_done:
            # nextEvent() method can be called with timeout to let
            # the program catch Ctrl-C between arrivals of new events
            event = session.nextEvent() # removed time out

            # Bloomberg will send us responses in chunks
            if event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                # self.logger.info("Processing Bloomberg Partial Response")
                data_frame_slice = self.process_response_event(event)
            elif event.eventType() == blpapi.Event.RESPONSE:
                # self.logger.info("Processing Bloomberg Full Response")
                data_frame_slice = self.process_response_event(event)
                not_done = False
            else:
                for msg in event:
                    if event.eventType() == blpapi.Event.SESSION_STATUS:
                        if msg.messageType() == self.SESSION_TERMINATED:
                            not_done = False

            # append DataFrame only if not empty
            if data_frame_slice is not None:
                if (data_frame.empty):
                    data_frame = data_frame_slice
                else:
                    # make sure we do not reattach a message we've already read
                    # sometimes Bloomberg can give us back the same message several times
                    # CAREFUL with this!
                    # compares the ticker names, and make sure they don't already exist in the time series
                    data_frame = self.combine_slices(data_frame, data_frame_slice)

        # make sure we do not have any duplicates in the time series
        if data_frame is not None:
            if data_frame.empty == False:
                try :
                    data_frame.drop_duplicates(keep='last')
                except:
                    self.logger.error("Failed : make sure we do not have any duplicates in the time series")

        self._data_frame = data_frame

    # process raw message returned by Bloomberg
    def process_response_event(self, event):

        data_frame = pandas.DataFrame()

        for msg in event:
            # generates a lot of output - so don't use unless for debugging purposes
            # self.logger.info(msg)

            if msg.hasElement(self.RESPONSE_ERROR):
                self.logger.error("REQUEST FAILED: " + str(msg.getElement(self.RESPONSE_ERROR)))
                continue

            data_frame_slice = self.process_message(msg)

            if (data_frame_slice is not None):
                if (data_frame.empty):
                    data_frame = data_frame_slice
                else:
                    data_frame = data_frame.append(data_frame_slice)
            else:
                data_frame = data_frame_slice

        return data_frame



    # create a session for Bloomberg with appropriate server & port
    def start_bloomberg_session(self):
        tries = 0

        session = None

        # try up to 5 times to start a session
        while(tries < 5):
            try:
                # fill SessionOptions
                sessionOptions = blpapi.SessionOptions()
                sessionOptions.setServerHost(Bloomberg_settings.BBG_SERVER_ADDRESS)
                sessionOptions.setServerPort(Bloomberg_settings.BBG_SERVER_PORT)

                self.logger.info("Starting Bloomberg session...")

                # create a Session
                session = blpapi.Session(sessionOptions)

                # start a Session
                if not session.start():
                    self.logger.error("Failed to start session.")
                    return

                self.logger.info("Returning session...")

                tries = 5
            except:
                tries = tries + 1

        # BBGLowLevelTemplate._session = session

        if session is None:
            self.logger.error("Failed to start session.")
            return


        return session

    def add_override(self, request, field, value):
        overrides = request.getElement("overrides")
        override1 = overrides.appendElement()
        override1.setElement("fieldId", field)
        override1.setElement("value", value)



    @abc.abstractmethod
    def process_message(self, msg):
        # to be implemented by subclass
        return

    # create request for data
    @abc.abstractmethod
    def send_bar_request(self, session, eventQueue):
        # to be implemented by subclass
        return

    # create request for data
    @abc.abstractmethod
    def combine_slices(self, data_frame, data_frame_slice):
        # to be implemented by subclass
        return

    def kill_session(self, session):
        if (session is not None):
            try:
                session.stop()

                self.logger.info("Stopping session...")
            finally:
                self.logger.info("Finally stopping session...")

            session = None
