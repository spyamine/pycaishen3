import abc

"""
this modules intend to solve the problem of datavendor specific options for fetching the data
It'll be mainly used from MarketDataRequest class in market request folder
"""

class IDataSourceOptions(object):
    """
    Interface for Datavendors specific options
    """
    def __init__(self):
        raise NotImplementedError

    def set_parameters(self):
        raise NotImplementedError


class BloombergOptions(IDataSourceOptions):
    """
    Class containing the overrides of Bloomberg used to set specific parameters
    """
    def __init__(self ):
        # self.datasource = "bloomberg"
        self.options_type = []
        self.options_fields = []
        self.options_values = []

    def set_parameters(self,options_type, options_fields, options_values):


        if type(options_type) == "str":

            options_field = [options_fields]
            options_values = [options_values]
            options_type = [options_type]

        self.options_type = options_type
        self.options_fields = options_fields
        self.options_values = options_values

    def __str__(self):

        i = 0
        s = "DataSourceOptions: \n"
        while (i < len(self.options_fields)):
            s = s + "* option ["+ str(i+1) +  "] type: "+ self.options_type[i] + " => "+  str(self.options_fields[i]) + ": " + str(self.options_values[i]) + "\n"
            i = i + 1
        return s

class IDataSourceOptionsFactory(object):
    "Abstract Datavendor Options Factory "
    @abc.abstractmethod
    def __call__(self, datasource):
        pass

class DataSourceOptionsFactory(IDataSourceOptionsFactory):
    """
    Concrete Datavendor options factory
    returns the correct BloombergOptions
    """
    def __call__(self, datasource):
        if datasource == "bloomberg" :
            return BloombergOptions()
        else :
            raise NotImplementedError

#######################################################################################################################

from pycaishen.util.loggermanager import LoggerManager


class OptionsBBG:

    # TODO create properties that cannot be manipulated
    def __init__(self):
        self.logger = LoggerManager().getLogger(__name__)


if __name__ == '__main__':

    options =  DataSourceOptionsFactory()
    print((options(datasource = "bloomberg",overrides = "l",overrides_values ="er")))