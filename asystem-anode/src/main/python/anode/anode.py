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
from twisted.internet import threads
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.defer import succeed
from twisted.internet.task import LoopingCall
from twisted.python import log
from twisted.web import client
from twisted.web.client import HTTPConnectionPool
from twisted.web.server import Site
from twisted.web.static import File

from plugin import DATUM_QUEUE_LAST
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

    def get_datums(self, datum_filter, datums=None):
        datums_filtered = {}
        if datums is None:
            for plugin_name, plugin in self.plugins.items():
                plugin.datums_filter_get(datums_filtered, datum_filter)
        else:
            Plugin.datums_filter(datums_filtered, datum_filter, datums)
        return datums_filtered

    def put_datums(self, datum_filter, data):
        if "sources" in datum_filter:
            for source in datum_filter["sources"]:
                if source in self.plugins:
                    self.plugins[source].push(data)

    def push_datums(self, datums):
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
            web = Site(web_root, logPath="/dev/null")
            web.noisy = False
            self.main_reactor.listenTCP(self.config["port"], web)
            self.main_reactor.run()
        return self


class WebWsFactory(WebSocketServerFactory):
    def __init__(self, url, anode):
        super(WebWsFactory, self).__init__(url)
        self.anode = anode
        self.ws_clients = []

    # noinspection PyShadowingNames
    def register(self, client):
        if client not in self.ws_clients:
            self.ws_clients.append(client)
            Log(logging.DEBUG).log("Interface", "state", lambda: "[ws] client registered [{}]".format(client.peer))

    def push(self, datums=None):
        for ws_client in self.ws_clients:
            ws_client.push(datums)

    def deregister(self, ws_client):
        if ws_client in self.ws_clients:
            self.ws_clients.remove(ws_client)
            Log(logging.DEBUG).log("Interface", "state", lambda: "[ws] client deregistered [{}]".format(ws_client.peer))


# noinspection PyPep8Naming
class WebWs(WebSocketServerProtocol):
    def __init__(self):
        super(WebWs, self).__init__()
        self.datum_filter = None

    def onConnect(self, request):
        self.datum_filter = request.params
        self.datum_filter["scope"] = [DATUM_QUEUE_LAST]
        Log(logging.DEBUG).log("Interface", "state", lambda: "[ws] connection request")

    def onOpen(self):
        Log(logging.DEBUG).log("Interface", "state", lambda: "[ws] connection opened")
        self.factory.register(self)
        self.push()

    def push(self, datums=None):
        log_timer = Log(logging.DEBUG).start()
        datums = self.factory.anode.get_datums(self.datum_filter, datums)
        Log(logging.DEBUG).log("Interface", "request", lambda: "[ws] push with filter [{}] and [{}] datums".format(
            self.datum_filter, 0 if datums is None else sum(len(datums_values) for datums_values in datums.values())))
        for datum in Plugin.datum_to_format(datums, "json")["json"]:
            self.sendMessage(datum, False)
        log_timer.log("Interface", "timer", lambda: "[ws]", context=self.push)

    def onClose(self, wasClean, code, reason):
        Log(logging.DEBUG).log("Interface", "state", lambda: "[ws] connection lost")
        self.factory.deregister(self)


# noinspection PyPep8Naming
class WebRest:
    server = Klein()

    def __init__(self, anode):
        self.anode = anode

    @server.route("/", methods=["POST"])
    def post(self, request):
        log_timer = Log(logging.DEBUG).start()
        datum_filter = urlparse.parse_qs(urlparse.urlparse(request.uri).query)
        Log(logging.DEBUG).log("Interface", "request", lambda: "[rest] post with filter [{}]".format(datum_filter))
        self.anode.put_datums(datum_filter, request.content.read())
        log_timer.log("Interface", "timer", lambda: "[rest]", context=self.post)
        return succeed(None)

    @server.route("/")
    @inlineCallbacks
    def get(self, request):
        log_timer = Log(logging.DEBUG).start()
        datum_filter = urlparse.parse_qs(urlparse.urlparse(request.uri).query)
        datums = self.anode.get_datums(datum_filter)
        Log(logging.DEBUG).log("Interface", "request", lambda: "[rest] get with filter [{}] and [{}] datums"
                               .format(datum_filter, sum(len(datums_values) for datums_values in datums.values())))
        datum_format = "json" if "format" not in datum_filter else datum_filter["format"][0]
        datums_formatted = yield threads.deferToThread(Plugin.datums_to_format, datums, datum_format, datum_filter, True)
        request.setHeader("Content-Disposition", "attachment; filename=anode." + datum_format)
        request.setHeader("Content-Type", ("application/" if datum_format != "csv" else "text/") + datum_format)
        log_timer.log("Interface", "timer", lambda: "[rest]", context=self.get)
        returnValue(datums_formatted)


class Log:
    def __init__(self, level=logging.INFO):
        self.level = level
        self.time_tracked = False
        self.time_real = 0
        self.time_user = 0
        self.time_real_start = None
        self.time_user_start = None

    @staticmethod
    def configure(verbose, quiet):
        # noinspection PyProtectedMember
        client._HTTP11ClientFactory.noisy = False
        if not logging.getLogger().handlers:
            logging_handler = logging.StreamHandler(sys.stdout)
            logging_handler.setFormatter(logging.Formatter(LOG_FORMAT))
            logging.getLogger().addHandler(logging_handler)
            if verbose:
                log.PythonLoggingObserver(loggerName=logging.getLogger().name).start()
        logging.getLogger().setLevel(logging.ERROR if quiet else (logging.DEBUG if verbose else logging.INFO))

    def start(self):
        if logging.getLogger().isEnabledFor(self.level):
            if self.time_user_start is not None:
                raise Exception("Log already started, cannot start")
            self.time_user_start = time.time()
            if self.time_real_start is None:
                self.time_real_start = self.time_user_start
        return self

    # noinspection PyUnboundLocalVariable
    def pause(self, stop=False):
        if logging.getLogger().isEnabledFor(self.level):
            if self.time_user_start is None:
                raise Exception("Log not started, cannot pause")
            time_now = time.time()
            self.time_user += int((time_now - self.time_user_start) * 1000)
            if stop:
                self.time_real = int((time_now - self.time_real_start) * 1000)
            self.time_tracked = True
            self.time_user_start = None
        return self

    def stop(self):
        if logging.getLogger().isEnabledFor(self.level):
            if self.time_user_start is not None:
                self.pause(True)
        return self

    def log(self, source, intonation, message, exception=None, context=None, off_thread=False):
        if logging.getLogger().isEnabledFor(self.level):
            self.stop()
            if self.level == logging.DEBUG:
                logger = logging.getLogger().debug
            elif self.level == logging.INFO:
                logger = logging.getLogger().info
            elif self.level == logging.WARN:
                logger = logging.getLogger().warning
            elif self.level == logging.ERROR:
                logger = logging.getLogger().error
            else:
                raise Exception("Unkown logging level [{}]".format(self.level))
            if not hasattr(message, '__call__'):
                raise Exception("Non callabled object [{}] passed as message".format(message))
            logger(" ".join(filter(None, [".".join([source, intonation]), message(),
                                          "" if context is None else "called [{}]".format(context.__name__),
                                          "" if not self.time_tracked else ("off-thread" if off_thread else "on-thread"),
                                          "" if not self.time_tracked else "real [{}] ms".format(self.time_real),
                                          "" if not self.time_tracked else "user [{}] ms".format(self.time_user)])))
            if exception is not None:
                logging.exception(exception)


def main(main_reactor=reactor, callback=None):
    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config", default="/etc/anode/anode.yaml", help="config FILE",
                      metavar="FILE")
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False, help="noisy output to stdout")
    parser.add_option("-q", "--quiet", action="store_true", dest="quiet", default=False, help="suppress all output to stdout")
    (options, args) = parser.parse_args()
    Log.configure(options.verbose, options.quiet)
    with open(options.config, "r") as stream:
        config = yaml.load(stream)
    return ANode(main_reactor, callback, options, config).start_server()
