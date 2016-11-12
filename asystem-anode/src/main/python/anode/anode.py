# -*- coding: utf-8 -*-

import logging
import logging.config
import os
import sys
import urlparse
from optparse import OptionParser

from autobahn.twisted.resource import WebSocketResource
from autobahn.twisted.websocket import WebSocketServerFactory, WebSocketServerProtocol
from klein import Klein
from klein.resource import KleinResource
from plugin import Plugin
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.python import log
from twisted.web.client import HTTPConnectionPool
from twisted.web.server import Site
from twisted.web.static import File

LOG_FORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"
WEB_PORT = 8080


class ANode:
    def __init__(self, main_reactor, callback, options):
        self.main_reactor = main_reactor
        self.callback = callback
        self.options = options
        self.plugins = []
        self.web_ws = WebWsFactory(u"ws://127.0.0.1:" + str(WEB_PORT), self)
        self.web_ws.protocol = WebWs
        self.web_rest = WebRest(self)
        self.web_pool = HTTPConnectionPool(reactor, persistent=True)

    def plugin(self, plugin_name, plugin_config):
        plugin_instance = Plugin.get(plugin_name, plugin_config)
        plugin_loopingcall = LoopingCall(plugin_instance.poll)
        plugin_loopingcall.clock = self.main_reactor
        plugin_loopingcall.start(plugin_config["poll"])
        return plugin_instance

    def datums(self, datum_filter, datum_scope="last"):
        datums = []
        for plugin in self.plugins:
            for data_metric in plugin.datums:
                if "metrics" not in datum_filter or data_metric.startswith(tuple(datum_filter["metrics"])):
                    for datum_type in plugin.datums[data_metric]:
                        if "types" not in datum_filter or datum_type.startswith(tuple(datum_filter["types"])):
                            for datum_bin in plugin.datums[data_metric][datum_type]:
                                if "bins" not in datum_filter or datum_bin.startswith(tuple(datum_filter["bins"])):
                                    datum = plugin.datums[data_metric][datum_type][datum_bin][datum_scope]
                                    if datum_scope != "last":
                                        datum = Plugin.datum_avro_to_dict(datum)
                                    datums.append(datum)
        return datums

    def start(self):
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Starting service ...")

        self.plugins.append(self.plugin("davis", {"poll": 1, "push": True, "pool": self.web_pool}))
        self.plugins.append(self.plugin("fronius", {"poll": 1, "push": False, "pool": self.web_pool}))
        self.plugins.append(self.plugin("netatmo", {"poll": 1, "push": True, "pool": self.web_pool}))
        self.plugins.append(self.plugin("publish", {"poll": 2, "push": True, "pool": self.web_pool}))
        self.plugins.append(self.plugin("callback", {"poll": 1, "callback": self.callback}))

        web_root = File(os.path.dirname(__file__) + "/web")
        web_root.putChild(b"push", WebSocketResource(self.web_ws))
        web_root.putChild(u"pull", KleinResource(self.web_rest.server))

        if self.main_reactor == reactor:
            self.main_reactor.listenTCP(WEB_PORT, Site(web_root))
            self.main_reactor.run()

        return self


class WebWsFactory(WebSocketServerFactory):
    def __init__(self, url, anode):
        WebSocketServerFactory.__init__(self, url)
        self.anode = anode


# noinspection PyPep8Naming
class WebWs(WebSocketServerProtocol):
    def __init__(self):
        super(WebWs, self).__init__()
        self.datum_filter = None

    def onConnect(self, request):
        self.datum_filter = request.params
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("WebSocket connection request [{}]".format(filter))

    def onOpen(self):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("WebSocket connected")
        for datum in self.factory.anode.datums(self.datum_filter):
            self.sendMessage(Plugin.datum_dict_to_json(datum), False)

    def onClose(self, wasClean, code, reason):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("WebSocket connection closed [{0}]".format(reason))


# noinspection PyPep8Naming
class WebRest:
    server = Klein()

    def __init__(self, anode):
        self.anode = anode

    @server.route("/")
    def onRequest(self, request):
        datum_filter = urlparse.parse_qs(urlparse.urlparse(request.uri).query)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("RESTful request [{0}]".format(datum_filter))


def main(main_reactor=reactor, callback=None):
    parser = OptionParser()
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="noisy output to stdout")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="suppress all output to stdout")
    (options, args) = parser.parse_args()
    if not logging.getLogger().handlers:
        logging_handler = logging.StreamHandler(sys.stdout)
        logging_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logging.getLogger().addHandler(logging_handler)
        log.PythonLoggingObserver(loggerName=logging.getLogger().name).start()
    logging.getLogger().setLevel(logging.CRITICAL if options.quiet else (logging.DEBUG if options.verbose else logging.INFO))
    return ANode(main_reactor, callback, options).start()
