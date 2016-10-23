import abc
from importlib import import_module

import anode.plugin


class Plugin(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        self.config = config
        if not self.config['quiet']:
            print str(type(self).__name__) + ":init"

    @abc.abstractmethod
    def loop(self):
        if not self.config['quiet']:
            print str(type(self).__name__) + ":loop"

    @staticmethod
    def get(module, config):
        return getattr(import_module("anode.plugin") if hasattr(anode.plugin, module.title()) else import_module("anode.plugin." + module), module.title())(config)


class Publish(Plugin):
    def __init__(self, config):
        super(self.__class__, self).__init__(config)

    def loop(self):
        super(self.__class__, self).loop()


class Callback(Plugin):
    def __init__(self, config):
        super(self.__class__, self).__init__(config)

    def loop(self):
        if self.config["callback"] is not None:
            self.config["callback"]()
        super(self.__class__, self).loop()
