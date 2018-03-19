__author__ = 'Mohamed Amine Guessous' # Mohamed Amine Guessous

#
# Copyright 2016
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
# License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# See the License for the specific language governing permissions and limitations under the License.
#

"""
IOEngineFactory

Returns IOEngine related to the specified engine using load_engine function
"""
from pycaishen.util import LoggerManager 
from pycaishen.util.settings import IO_configuration as IO_Settings
from pycaishen.util.deprecated_warnings import deprecated

from pycaishen.util.settings import IO_configuration as settings

class IOEngineFactory(object):
    def __init__(self):
        """

        :param engine: name of the engine needed
        """
        #self.config = ConfigManager()
        if IO_Settings.IO_VERBOSE_MODE:
            self.logger = LoggerManager().getLogger(__name__)

    def __call__(self, engine):
        try:
            engine = str(engine).lower()
            valid_data_source = settings.VALID_IO

            if not engine in valid_data_source:
                self.logger.warning(engine & " is not a defined IO framework.")
        except: pass



        if engine == 'arctic':
            from pycaishen.ioengines.ioenginearctic import IOEngineArctic
            return IOEngineArctic()

        if engine == "excel":
            raise NotImplementedError

        if engine == "hd5":
            raise NotImplementedError

        if engine == "mysql":
            raise NotImplementedError

        else:
            raise NotImplementedError


    @deprecated
    def load_engine(self,engine):

        try:
            engine = str(engine).lower()
            valid_data_source = settings.VALID_IO

            if not engine in valid_data_source:
                self.logger.warning(engine & " is not a defined IO framework.")
        except:
            pass

        if engine == 'arctic':
            from pycaishen.ioengines.ioenginearctic import IOEngineArctic
            return IOEngineArctic()

        if engine == "excel":
            raise NotImplementedError

        if engine == "hd5":
            raise NotImplementedError

        if engine == "mysql":
            raise NotImplementedError

        else:
            raise NotImplementedError

