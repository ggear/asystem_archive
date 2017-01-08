# -*- coding: utf-8 -*-

import logging
import logging.config
import os
import sys
import time
import urlparse
from optparse import OptionParser

import yaml
from autobahn.twisted.resource import WebSocketResource
from autobahn.twisted.websocket import WebSocketServerFactory, WebSocketServerProtocol
from klein import Klein
from klein.resource import KleinResource
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.defer import succeed
from twisted.internet.task import LoopingCall
from twisted.python import log
from twisted.web.client import HTTPConnectionPool
from twisted.web.server import Site
from twisted.web.static import File

from plugin import Plugin

LOG_FORMAT = "%(asctime)s %(name)-12s %(levelname)-8s %(message)s"


class ANode:
    def __init__(self, main_reactor, callback, options, config):
        self.main_reactor = main_reactor
        self.callback = callback
        self.options = options
        self.config = config
        self.plugins = {}
        self.web_ws = WebWsFactory(u"ws://" + self.config["host"] + ":" + str(self.config["port"]), self)
        self.web_ws.protocol = WebWs
        self.web_rest = WebRest(self)
        self.web_pool = HTTPConnectionPool(reactor, persistent=True)

    def register_plugin(self, plugin_name, plugin_config):
        plugin_instance = Plugin.get(self, plugin_name, plugin_config)
        if plugin_config["poll_seconds"] > 0:
            plugin_loopingcall = LoopingCall(plugin_instance.poll)
            plugin_loopingcall.clock = self.main_reactor
            plugin_loopingcall.start(plugin_config["poll_seconds"])
        return plugin_instance

    def get_datums(self, datum_filter, datum_format="dict", datums=None):
        datums_filtered = []
        if datums is None:
            for plugin_name, plugin in self.plugins.items():
                datums_filtered.extend(plugin.datums_filter_get(datum_filter, datum_format))
        else:
            datums_filtered.extend(Plugin.datums_filter(datum_filter, datums, datum_format))
        if "limit" in datum_filter and min(datum_filter["limit"]).isdigit() and int(min(datum_filter["limit"])) <= len(datums_filtered):
            datums_filtered = datums_filtered[:int(min(datum_filter["limit"]))]
        return Plugin.datums_sort(datums_filtered)

    def push_datums(self, datum_filter, data):
        if "sources" in datum_filter:
            for source in datum_filter["sources"]:
                if source in self.plugins:
                    self.plugins[source].push(data)

    def publish_datums(self, datums):
        self.web_ws.push(datums)

    def start_server(self):
        for plugin_name in self.config["plugin"]:
            self.config["plugin"][plugin_name]["pool"] = self.web_pool
            self.plugins[plugin_name] = self.register_plugin(plugin_name, self.config["plugin"][plugin_name])
        self.plugins["callback"] = self.register_plugin("callback", {"poll_seconds": self.config["callback_poll_seconds"], "callback": self.callback})
        web_root = File(os.path.dirname(__file__) + "/web")
        web_root.putChild(b"ws", WebSocketResource(self.web_ws))
        web_root.putChild(u"rest", KleinResource(self.web_rest.server))
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
                logging.getLogger().debug("state\t\tInterface [ws] client registered [{}]".format(client.peer))

    def push(self, datums=None):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            time_start = time.time()
        for client in self.clients:
            client.push(datums)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("perf\t\tInterface [ws] push on-thread [{}] ms".format(str(int((time.time() - time_start) * 1000))))

    def deregister(self, client):
        if client in self.clients:
            self.clients.remove(client)
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.getLogger().debug("state\t\tInterface [ws] client deregistered [{}]".format(client.peer))


# noinspection PyPep8Naming
class WebWs(WebSocketServerProtocol):
    def __init__(self):
        super(WebWs, self).__init__()
        self.datum_filter = None

    def onConnect(self, request):
        self.datum_filter = request.params
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("state\t\tInterface [ws] connection request")

    def onOpen(self):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("state\t\tInterface [ws] connection opened")
        self.factory.register(self)
        self.push()

    def push(self, datums=None):
        datums = self.factory.anode.get_datums(self.datum_filter, "dict", datums)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("state\t\tInterface [ws] push with filter [{}] and [{}] datums".format(
                self.datum_filter, 0 if datums is None else len(datums)))
        for datum in datums:
            self.sendMessage(Plugin.datum_dict_to_json(datum), False)

    def onClose(self, wasClean, code, reason):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("state\t\tInterface [ws] connection lost")
        self.factory.deregister(self)


# noinspection PyPep8Naming
class WebRest:
    server = Klein()

    def __init__(self, anode):
        self.anode = anode

    @server.route("/", methods=["POST"])
    def post(self, request):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            time_start = time.time()
        datum_filter = urlparse.parse_qs(urlparse.urlparse(request.uri).query)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("state\t\tInterface [rest] push with filter [{}]".format(datum_filter))
        self.anode.push_datums(datum_filter, request.content.read())
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("perf\t\tInterface [rest] post on-thread [{}] ms".format(str(int((time.time() - time_start) * 1000))))
        return succeed(None)

    @server.route("/")
    @inlineCallbacks
    def get(self, request):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            time_start = time.time()
        datum_filter = urlparse.parse_qs(urlparse.urlparse(request.uri).query)
        datums_filtered = self.anode.get_datums(datum_filter, "dict")
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("state\t\tInterface [rest] pull with filter [{}] and [{}] datums".format(datum_filter, len(datums_filtered)))
        datum_format = "json" if "format" not in datum_filter else datum_filter["format"][0]
        datums_format = yield Plugin.datums_dict_to_format(datums_filtered, datum_format)
        request.setHeader("Content-Disposition", "attachment; filename=anode." + datum_format)
        request.setHeader("Content-Type", ("application/" if datum_format != "csv" else "text/") + datum_format)
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("perf\t\tInterface [rest] get on-thread [{}] ms".format(str(int((time.time() - time_start) * 1000))))
        returnValue(datums_format)


def main(main_reactor=reactor, callback=None):
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config", default="/etc/anode/anode.yaml", help="config FILE",
                      metavar="FILE")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="noisy output to stdout")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="suppress all output to stdout")
    (options, args) = parser.parse_args()
    if not logging.getLogger().handlers:
        logging_handler = logging.StreamHandler(sys.stdout)
        logging_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logging.getLogger().addHandler(logging_handler)
        log.PythonLoggingObserver(loggerName=logging.getLogger().name).start()
    logging.getLogger().setLevel(logging.ERROR if options.quiet else (logging.DEBUG if options.verbose else logging.INFO))
    with open(options.config, "r") as stream:
        config = yaml.load(stream)
    return ANode(main_reactor, callback, options, config).start_server()
