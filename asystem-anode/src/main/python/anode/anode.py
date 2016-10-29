from optparse import OptionParser

from plugin import Plugin
from twisted.internet import reactor
from twisted.internet.task import LoopingCall


class ANode:
    def __init__(self, clock, callback, options):
        self.clock = clock
        self.callback = callback
        self.options = options
        self.plugins = []

    def plugin(self, plugin_name, plugin_config):
        plugin = LoopingCall(Plugin.get(plugin_name, plugin_config).loop)
        plugin.clock = self.clock
        plugin.start(plugin_config['poll'])
        return plugin_name

    def start(self):
        self.plugins.append(self.plugin("davis", {"quiet": self.options.quiet, "poll": 1}))
        self.plugins.append(self.plugin("fronius", {"quiet": self.options.quiet, "poll": 1}))
        self.plugins.append(self.plugin("netatmo", {"quiet": self.options.quiet, "poll": 1}))
        self.plugins.append(self.plugin("publish", {"quiet": self.options.quiet, "poll": 1}))
        self.plugins.append(self.plugin("callback", {"quiet": self.options.quiet, "poll": 1, "callback": self.callback}))
        if hasattr(self.clock, 'run'):
            self.clock.run()


def main(clock=reactor, callback=None):
    parser = OptionParser()
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="suppress output to stdout")
    (options, args) = parser.parse_args()
    ANode(clock, callback, options).start()
