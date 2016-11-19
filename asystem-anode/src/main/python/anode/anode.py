# -*- coding: utf-8 -*-

import logging
import logging.config
import os
import sys
import urlparse
from optparse import OptionParser

import yaml
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


class ANode:
    def __init__(self, main_reactor, callback, options, config):
        self.main_reactor = main_reactor
        self.callback = callback
        self.options = options
        self.config = config
        self.plugins = []
        self.web_ws = WebWsFactory(u"ws://" + self.config["host"] + ":" + str(self.config["port"]), self)
        self.web_ws.protocol = WebWs
        self.web_rest = WebRest(self)
        self.web_pool = HTTPConnectionPool(reactor, persistent=True)

    def plugin(self, plugin_name, plugin_config):
        plugin_instance = Plugin.get(self, plugin_name, plugin_config)
        plugin_loopingcall = LoopingCall(plugin_instance.poll)
        plugin_loopingcall.clock = self.main_reactor
        plugin_loopingcall.start(plugin_config["poll"])
        return plugin_instance

    def datums_filter_get(self, datum_filter, datum_format="dict"):
        datums_filtered = []
        for plugin in self.plugins:
            datums_filtered.extend(plugin.datums_filter_get(datum_filter, datum_format))
        return datums_filtered

    def datums_push(self, datums):
        self.web_ws.push(datums)

    def start(self):
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Starting service ...")
        for plugin in self.config["plugin"]:
            self.config["plugin"][plugin]["pool"] = self.web_pool
            self.plugins.append(self.plugin(plugin, self.config["plugin"][plugin]))
        self.plugins.append(self.plugin("callback", {"poll": 1, "callback": self.callback}))
        web_root = File(os.path.dirname(__file__) + "/web")
        web_root.putChild(b"push", WebSocketResource(self.web_ws))
        web_root.putChild(u"pull", KleinResource(self.web_rest.server))
        if self.main_reactor == reactor:
            self.main_reactor.listenTCP(self.config["port"], Site(web_root))
            self.main_reactor.run()
        return self


class WebWsFactory(WebSocketServerFactory):
    def __init__(self, url, anode):
        super(WebWsFactory, self).__init__(url)
        self.anode = anode
        self.clients = []

    def register(self, client):
        if client not in self.clients:
            self.clients.append(client)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.getLogger().debug("WebSocket client registered [{}]".format(client.peer))

    def push(self, datums=None):
        for client in self.clients:
            client.push(datums)

    def deregister(self, client):
        if client in self.clients:
            self.clients.remove(client)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.getLogger().debug("WebSocket client deregistered [{}]".format(client.peer))


# noinspection PyPep8Naming
class WebWs(WebSocketServerProtocol):
    def __init__(self):
        super(WebWs, self).__init__()
        self.datum_filter = None

    def onConnect(self, request):
        self.datum_filter = request.params
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("WebSocket connection request [{}]".format(request))

    def onOpen(self):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("WebSocket connection opened")
        self.factory.register(self)
        self.push()

    def push(self, datums=None):
        for datums in Plugin.datums_filter(self.datum_filter, datums, "json") if datums is not None else \
                self.factory.anode.datums_filter_get(self.datum_filter, "json"):
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.getLogger().debug("WebSocket push with filter [{}] producing [{}] datums".format(self.datum_filter, len(datums)))
            self.sendMessage(datums, False)

    def onClose(self, wasClean, code, reason):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("WebSocket connection lost")
        self.factory.deregister(self)


# noinspection PyPep8Naming
class WebRest:
    server = Klein()

    def __init__(self, anode):
        self.anode = anode

    @server.route("/")
    def onRequest(self, request):
        datum_filter = urlparse.parse_qs(urlparse.urlparse(request.uri).query)
        datums = self.anode.datums_filter_get(datum_filter, "json")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("RESTful pull with filter [{}] producing [{}] datums".format(datum_filter, len(datums)))
        return datums


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
    with open(os.path.dirname(__file__) + "/anode.yaml", 'r') as stream:
        config = yaml.load(stream)
    return ANode(main_reactor, callback, options, config).start()
