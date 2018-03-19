__author__ = 'saeedamen' # Saeed Amen

#
# Copyright 2016 Cuemacro
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
LoggerManager

Acts as a wrapper for logging.

"""

import logging
import logging.config

logging.info('Starting logger for...')


from pycaishen.util.settings import Logging_configuration as Log_Settings
from pycaishen.util.singleton import Singleton

class LoggerManager(object, metaclass=Singleton):
    _loggers = {}

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def getLogger(name=None):
        if not name:
            try:
                logging.config.fileConfig(Log_Settings.LOGGING_CONF_FILE)
            except: pass

            log = logging.getLogger();
        elif name not in list(LoggerManager._loggers.keys()):
            try:
                logging.config.fileConfig(Log_Settings.LOGGING_CONF_FILE)
            except: pass

            LoggerManager._loggers[name] = logging.getLogger(str(name))

        log = LoggerManager._loggers[name]

        # when recalling appears to make other loggers disabled
        # hence apply this hack!
        for name in list(LoggerManager._loggers.keys()):
            LoggerManager._loggers[name].disabled = False

        return log

if __name__ == '__main__':

    logger = LoggerManager.getLogger(__name__)
    # logger.setLevel(logging.INFO)

    logger.info("Hello")