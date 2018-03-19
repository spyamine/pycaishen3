


from pycaishen.ioengines.ioenginefactory import IOEngineFactory
from pycaishen.util.loggermanager import LoggerManager
from pycaishen.util.settings import configurer as SETTINGS
from pycaishen.util.settings import IO_configuration as IO_SETTINGS



class PycaishenStorage(object):
    """
    The Pycaishen class is a top-level God object. Responsible for handling different storage engines
    """

    def __init__(self, engine, app_name=SETTINGS.APPLICATION_NAME):
        """

        :param engine: name of the engine
        :param app_name: optional, already defined
        """
        self._application_name = app_name
        self._logger = LoggerManager().getLogger(__name__)
        # sets the self._io_engine parameter of the class. we use the IOEngineFactory class
        self.set_IO_Engine(engine)


    def __str__(self):
        # return "<Pycaishen Storage at %s using as an engine: %s>" % (hex(id(self)),str(self._io_engine))
        return "<Pycaishen Storage at %s >" % (hex(id(self)))
    def __repr__(self):
        return str(self)



    def set_IO_Engine(self,engine):
        """
        Change the engine used to avoid creating too many PycaishenStorage classes
        :param engine: name of the engine
        :return:
        """
        if engine is not None:
            if type(engine) == type("str"):
                engine = str(engine).lower()
                if engine in IO_SETTINGS.VALID_IO:
                    IO = IOEngineFactory()
                    IO = IO(engine)
                    self._io_engine = IO
                    self._logger.info("Loading engine : %s" % engine)
                else:
                    self._logger.warn("Invalid engine name provided")
                    self._logger.info("Valid engines are : %s" % str(IO_SETTINGS.VALID_IO).strip('[]'))

    def write(self, symbol, data_frame, library_name, append_data=False, metadata=None):
        """

        :param symbol: symbol
        :param data_frame: data in form of a dataframe
        :param library_name: name of the library
        :param append_data: True or False. default is False
        :param metadata: dictionary form and it is optional
        :return:
        """
        self._io_engine.write( symbol, data_frame, library_name, append_data, metadata)

    def read(self, symbol, library, start_date=None, finish_date=None, metadata=None):
        """
        returns the data in form of a dataframe for the parameters specified
        :param symbol: symbol
        :param library: library name
        :param start_date: optional
        :param finish_date: optional
        :param metadata: optional
        :return: dataframe containing the data
        """
        return self._io_engine.read( symbol, library, start_date, finish_date, metadata)

    def delete_symbol(self, symbol, library):
        """
        delete the data for the symbol specified in the library specified
        :param symbol:
        :param library: name of the library
        :return:
        """
        self._io_engine.delete_symbol(symbol,library)

    def delete_library(self, library):
        """
        deletes the library selected
        :param library:
        :return:
        """
        self._io_engine.delete_library(library)

    def list_symbols(self, library, key=None):
        """
        return a list of available symbols in the engine host
        :param library: name of library
        :param key:
        :return: list of symbols
        """
        return self._io_engine.list_symbols(library,key)

    def list_libraries(self):
        """
        the function gets the list of available libraries in the engine host
        :return: List of libraries names available
        """
        return self._io_engine.list_libraries()

    def has_symbol(self, symbol, library):
        """
        Check if the symbol exist in the library
        :param symbol:
        :param library: library name
        :return: True or False
        """
        return self._io_engine.has_symbol(symbol, library)


