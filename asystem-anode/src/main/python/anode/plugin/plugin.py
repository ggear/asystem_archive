import abc
from importlib import import_module


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
        return getattr(import_module("anode.plugin" + ((".%s" % module) if module != "poll" else "")), module.title())(config)


class Poll(Plugin):
    def __init__(self, config):
        super(self.__class__, self).__init__(config)

    def loop(self):
        if self.config["callback"] is not None:
            self.config["callback"]()
        super(self.__class__, self).loop()
