from __future__ import print_function
from __future__ import print_function

import abc
from importlib import import_module

import anode.plugin


class Plugin(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, config):
        self.config = config
        if not self.config['quiet']:
            print(str(type(self).__name__) + ":init")

    @abc.abstractmethod
    def loop(self):
        if not self.config['quiet']:
            print(str(type(self).__name__) + ":loop")

    @staticmethod
    def get(module, config):
        return getattr(import_module("anode.plugin") if hasattr(anode.plugin, module.title()) else import_module("anode.plugin." + module), module.title())(config)


class Publish(Plugin):
    def loop(self):
        super(self.__class__, self).loop()


class Callback(Plugin):
    def loop(self):
        if self.config["callback"] is not None:
            self.config["callback"]()
