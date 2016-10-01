from optparse import OptionParser

from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from plugin import Plugin


class ANode():
    def __init__(self, clock, options):
        self.clock = clock
        self.options = options
        self.plugins = []

    def plugin(self, plugin, config):
        plugin = LoopingCall(Plugin.get(plugin, config).loop)
        plugin.clock = self.clock
        plugin.start(config['poll'])
        return plugin

    def start(self):
        self.plugins.append(self.plugin("davis", {"quiet": self.options.quiet, "poll": 1}))
        self.plugins.append(self.plugin("fronius", {"quiet": self.options.quiet, "poll": 1}))
        self.plugins.append(self.plugin("netatmo", {"quiet": self.options.quiet, "poll": 1}))
        if hasattr(self.clock, 'run'):
            self.clock.run()


def main(clock=reactor):
    parser = OptionParser()
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="suppress output to stdout")
    (options, args) = parser.parse_args()
    ANode(clock, options).start()
