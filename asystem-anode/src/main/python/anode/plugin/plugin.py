import abc
from importlib import import_module


class Plugin(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        self.config = config
        if not self.config['quiet']:
            print __name__ + ":init"

    @abc.abstractmethod
    def loop(self):
        if not self.config['quiet']:
            print __name__ + ":loop"

    @staticmethod
    def get(module, config):
        return getattr(import_module("anode.plugin.%s" % module), module.title())(config)
