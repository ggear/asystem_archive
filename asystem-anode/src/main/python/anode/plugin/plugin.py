import abc
from importlib import import_module


class Plugin(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def loop(self):
        return

    @staticmethod
    def get(module, config):
        return getattr(import_module("anode.plugin.%s" % module), module.title())(config)
