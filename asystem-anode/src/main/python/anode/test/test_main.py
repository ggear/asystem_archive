# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import print_function

import calendar
import datetime
import json
import os.path
import os.path
import shutil
import sys
import time
import urlparse
from StringIO import StringIO
from random import randint

import ilio
import pandas
import treq
from twisted.internet import threads
from twisted.internet.defer import succeed
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase

from anode.anode import MqttPublishService
from anode.anode import Plugin
from anode.anode import main


# noinspection PyPep8Naming, PyUnresolvedReferences, PyShadowingNames,PyPep8,PyTypeChecker
class ANodeTest(TestCase):
    def setUp(self):
        self.patch(treq, "get", lambda url, timeout=0, pool=None: MockHttpResponse(url))
        self.patch(treq, "post", lambda url, data, timeout=0, pool=None: MockHttpResponse(url))
        self.patch(treq, "text_content", lambda response: MockHttpResponseContent(response))
        self.patch(threads, "deferToThread", lambda to_execute, *arguments, **keyword_arguments: to_execute(*arguments, **keyword_arguments))
        self.patch(MqttPublishService, "startService", lambda myself: None)
        self.patch(MqttPublishService, "isConnected", lambda myself: True)
        self.patch(MqttPublishService, "publishMessage", lambda myself, message, queue, on_failure: succeed(None))
        shutil.rmtree(DIR_ANODE, ignore_errors=True)
        os.makedirs(DIR_ANODE)
        shutil.rmtree(DIR_ANODE_TEST, ignore_errors=True)
        os.makedirs(DIR_ANODE_TEST)
        print("")

    @staticmethod
    def clock_tick(anode, period, periods, skip_ticks=False, skip_all=False):
        if skip_all:
            test_clock.advance(period * periods)
        else:
            global test_ticks
            if test_ticks == 1:
                ANodeTest.clock_tock(anode)
            for tickTock in range((period * (periods - 1)) if skip_ticks else 0, period * periods, period):
                test_clock.advance((period * periods) if skip_ticks else period)
                ANodeTest.clock_tock(anode)
                test_ticks += 1

    @staticmethod
    def clock_tock(anode):
        for source in HTTP_POSTS:
            anode.put_datums({"sources": [source]}, ANodeTest.template_populate(HTTP_POSTS[source]))

    @staticmethod
    def template_populate(template):
        integer = test_ticks % TEMPLATE_INTEGER_MAX
        epoch = (1 if test_clock.seconds() == 0 else int(test_clock.seconds())) + TIME_START_OF_DAY
        if integer == 0:
            integer = 1 if test_repeats else TEMPLATE_INTEGER_MAX
        if test_repeats and integer % 2 == 0:
            integer = 1
        if test_nulls:
            integer = "null"
        if test_randomise:
            integer = randint(1, TEMPLATE_INTEGER_MAX - 1)
        epoch_str = str(epoch)
        time_str = datetime.datetime.fromtimestamp(epoch).strftime('%-I:%M %p AWST') if epoch != "null" else "null"
        timestamp_str = datetime.datetime.fromtimestamp(epoch).strftime('%Y-%m-%dT%H:%M:%S+08:00') if epoch != "null" else "null"
        integer_str = str(integer)
        floatingpoint_str = (str(integer) + "." + str(integer)) if integer != "null" else "null"
        return template if test_corruptions else template \
            .replace("\\\"${EPOCH}\\\"", epoch_str if epoch_str != "null" else "") \
            .replace("\"${EPOCH}\"", epoch_str) \
            .replace("\\\"${TIME}\\\"", time_str if time_str != "null" else "") \
            .replace("\"${TIME}\"", time_str) \
            .replace("\\\"${TIMESTAMP}\\\"", timestamp_str if timestamp_str != "null" else "") \
            .replace("\"${TIMESTAMP}\"", timestamp_str) \
            .replace("\\\"${INTEGER}\\\"", integer_str if integer_str != "null" else "") \
            .replace("\"${INTEGER}\"", integer_str) \
            .replace("\\\"${-INTEGER}\\\"", (("-" if integer_str != "null" else "") + integer_str) if integer_str != "null" else "") \
            .replace("\"${-INTEGER}\"", ("-" if integer_str != "null" else "") + integer_str) \
            .replace("\\\"${FLOATINGPOINT}\\\"", floatingpoint_str if floatingpoint_str != "null" else "") \
            .replace("\"${FLOATINGPOINT}\"", floatingpoint_str) \
            .replace("\\\"${-FLOATINGPOINT}\\\"",
                     (("-" if floatingpoint_str != "null" else "") + floatingpoint_str) if floatingpoint_str != "null" else "") \
            .replace("\"${-FLOATINGPOINT}\"", ("-" if floatingpoint_str != "null" else "") + floatingpoint_str)

    @staticmethod
    def unwrap_defered(defered):
        return getattr(defered, 'result', "")

    @staticmethod
    def rest(anode, url):
        return ANodeTest.unwrap_defered(anode.web_rest.get(MockRequest(url)))

    @staticmethod
    def rest_json(anode, url):
        response_text = ANodeTest.rest(anode, url)
        response_json = json.loads(response_text)
        return response_json, len(response_json), json.dumps(response_json, sort_keys=True, indent=4, separators=(',', ': '))

    @staticmethod
    def rest_csv(anode, url):
        response_print = urlparse.parse_qs(urlparse.urlparse(url).query)["print"][0] \
            if "print" in urlparse.parse_qs(urlparse.urlparse(url).query) else "not-pretty"
        response_text = ANodeTest.rest(anode, url)
        response_df = pandas.read_csv(StringIO(response_text)) \
            if response_text is not None and isinstance(response_text, basestring) and len(response_text) > 0 else pandas.DataFrame()
        return response_df, \
               sum(response_df[response_df_column].count() for response_df_column in response_df if
                   response_df_column.startswith("data_value")) if response_print != "pretty" else sum(
                   response_df[response_df_column].count() for response_df_column in response_df if
                   (not response_df_column.startswith("bin_timestamp")) and not response_df_column.startswith("Time")), response_text

    @staticmethod
    def rest_svg(anode, url):
        global test_svg_counter
        test_svg_counter += 1
        response_svg = ANodeTest.rest(anode, url)
        response_csv = ANodeTest.rest_csv(anode, url.replace("format=svg", "format=csv"))
        response_svg_file = DIR_ANODE_TEST + "anode_" + str(test_svg_counter) + ".svg"
        file(response_svg_file, "w").write(response_svg)
        return response_csv[0], response_csv[1], "file://" + FILE_SVG_HTML + "?dir=" + DIR_ANODE_TEST + "&count=" + str(test_svg_counter + 1) + \
               "#" + str(test_svg_counter)

    def metrics_count(self, anode):
        metrics = 0
        metrics_anode = 0
        for metric in self.rest_json(anode, "/rest/?metrics=anode")[0]:
            metrics_anode += 1
            if metric["data_metric"].endswith("metrics"):
                metrics += metric["data_value"]
        return metrics, metrics_anode

    def assertRest(self, assertion, anode, url, assertions=True, log=False):
        response = None
        exception_raised = None
        response_format = "json"
        try:
            response_format = urlparse.parse_qs(urlparse.urlparse(url).query)["format"][0] \
                if "format" in urlparse.parse_qs(urlparse.urlparse(url).query) else response_format
            if response_format == "json":
                response = self.rest_json(anode, url)
            elif response_format == "csv":
                response = self.rest_csv(anode, url)
            elif response_format == "svg":
                response = self.rest_svg(anode, url)
        except Exception as exception:
            log = True
            exception_raised = exception
        if log or (assertions and assertion != response[1]):
            print("RESTful [{}] {} response:\n{}".format(url, response_format.upper(), "" if response is None else response[2]))
            print("RESTful [{}] {} response includes [{}] datums".format(url, response_format.upper(), 0 if response is None else response[1]))
        if exception_raised is not None:
            raise exception_raised
        if assertions:
            self.assertEquals(assertion, response[1])
        return response

    def anode_init(self, radomise=True, repeats=False, nulls=False, corruptions=False, period=1, iterations=1):
        global test_ticks
        global test_randomise
        global test_repeats
        global test_nulls
        global test_corruptions
        global test_clock
        test_ticks = 1
        test_clock = Clock()
        test_randomise = radomise
        test_repeats = repeats
        test_nulls = nulls
        test_corruptions = corruptions
        anode = main(test_clock)
        self.assertTrue(anode is not None)
        if iterations > 0:
            self.clock_tick(anode, period, iterations)
        return anode

    def test_encode(self):
        for unescaped, escaped in {
            "": "",
            ".": "__",
            "_": "_X",
            "-": "_D",
            "!": "_P21",
            "*": "_P2A",
            "(": "_P28",
            ")": "_P29",
            "#": "_P23",
            "@": "_P40",
            "_J": "_XJ",
            "_D": "_XD",
            ".D": "__D",
            "_X": "_XX",
            "_X.": "_XX__",
            "_X._": "_XX___X",
            "_P%": "_XP_P25",
            "_X_X$\/..%_D_": "_XX_XX_P24_P5C_P2F_____P25_XD_X",
            "$": "_P24",
            "%": "_P25",
            "m/s": "m_P2Fs",
            "km/h": "km_P2Fh",
            "mm/h": "mm_P2Fh",
            "°": "_PC2_PB0",
            "°C": "_PC2_PB0C",
            "anode.fronius.metrics": "anode__fronius__metrics",
            "energy.consumption-off-peak-morning.grid": "energy__consumption_Doff_Dpeak_Dmorning__grid"
        }.iteritems():
            self.assertEqual(unescaped, Plugin.datum_field_decode(escaped))
            self.assertEqual(escaped, Plugin.datum_field_encode(unescaped))
            self.assertEqual(escaped, Plugin.datum_field_encode(Plugin.datum_field_decode(escaped)))
            self.assertEqual(unescaped, Plugin.datum_field_decode(Plugin.datum_field_encode(unescaped)))
            self.assertEqual(unescaped, Plugin.datum_field_decode(Plugin.datum_field_encode(Plugin.datum_field_decode(escaped))))
            self.assertEqual(escaped, Plugin.datum_field_encode(Plugin.datum_field_decode(Plugin.datum_field_encode(unescaped))))

    def test_bare(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_BARE, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, iterations=5)
        self.assertRest(0, anode, "/rest", False)

    def test_null(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, True, False)
        self.assertRest(0, anode, "/rest", False)

    def test_corrupt(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, True)
        self.assertRest(0, anode, "/rest", False)

    def test_random(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-v"])
        anode = self.anode_init(True, True, False, False, iterations=5)
        self.assertRest(0, anode, "/rest", False)

    def test_all(self):
        for arguments in [
            ["anode", "--config=" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE],
            ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-v"],
            ["anode", "--config=" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "--verbose"],
            ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-q"],
            ["anode", "--config=" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "--quiet"]
        ]:
            self.patch(sys, "argv", arguments)
            anode = self.anode_init(False, True, False, False)
            metrics, metrics_anode = self.metrics_count(anode)
            for filter_scope in [None, "last", "publish", "history"]:
                for filter_format in [None, "json", "csv"]:
                    anode = self.anode_init(False, True, False, False)
                    self.assertRest(0,
                                    anode,
                                    "/rest/?metrics=some.fake.metric" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0,
                                    anode,
                                    "/rest/?metrics=some.fake.metric&metrics=some.other.fake.metric" + (
                                        ("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0,
                                    anode,
                                    "/rest/?metrics=power&types=fake.type" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0,
                                    anode,
                                    "/rest/?metrics=power&units=°" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0,
                                    anode,
                                    "/rest/?metrics=power&types=point&bins=fake_bin" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0,
                                    anode,
                                    "/rest/?metrics=power&types=point&bins=fake_bin&print=fake_print" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0,
                                    anode,
                                    "/rest/?metrics=power&types=point&bins=fake_bin&print=pretty" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0,
                                    anode,
                                    "/rest/?metrics=rain-rate&types=mean&units=mm" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=rain-rate&types=mean&units=mm/h" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&bins=1second" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&types=point" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&types=point&bins=1second" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&metrics=&types=point&bins=1second" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&metrics=some.fake.metric&metrics=&types=point&bins=1second" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&metrics=&types=point&types=fake.type&bins=1second" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&metrics=&types=point&bins=1second&bins=fake_bin" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&metrics=&types=point&bins=1second&units=W" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&metrics=&types=point&bins=1second&units=W&print=pretty" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 2,
                                    anode,
                                    "/rest/?metrics=power.production.inverter&metrics=power.export.grid&metrics=&types=point&bins=1second" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 3,
                                    anode,
                                    "/rest/?metrics=power.production.inverter" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 3,
                                    anode,
                                    "/rest/?metrics=wind-gust-bearing.outdoor.roof&units=°" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 20,
                                    anode,
                                    "/rest/?metrics=energy&print=pretty" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 16,
                                    anode,
                                    "/rest/?bins=2second" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 21,
                                    anode,
                                    "/rest/?metrics=power" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 21,
                                    anode,
                                    "/rest/?metrics=power&print=pretty" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if (filter_scope == "publish") else (metrics if filter_scope != "history" else (metrics - metrics_anode)),
                                    anode,
                                    "/rest/?something=else" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if (filter_scope == "publish") else (metrics if filter_scope != "history" else (metrics - metrics_anode)),
                                    anode,
                                    "/rest/?something=else&print=pretty" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if (filter_scope == "publish") else (metrics if filter_scope != "history" else (metrics - metrics_anode)),
                                    anode,
                                    "/rest/?" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)

    def test_wide(self):
        metrics_period10 = 401
        metrics_period10_fill = 1066
        metrics_period5_fill = 2091
        metrics_period20 = 221
        metrics_period20_fill = 533
        for config in [
            FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL,
            FILE_CONFIG_FRONIUS_UNBOUNDED_LARGE
        ]:
            self.patch(sys, "argv", ["anode", "-c" + config, "-d" + DIR_ANODE, "-q"])
            anode = self.anode_init(False, False, False, False, period=10, iterations=25)
            for filter_method in [None, "min", "max"]:
                for fitler_fill in [None, "zeros", "linear"]:
                    self.assertRest(metrics_period10 if fitler_fill is None else metrics_period10_fill,
                                    anode,
                                    "/rest/?scope=history&format=csv&print=pretty" +
                                    (("&method=" + filter_method) if filter_method is not None else "") +
                                    (("&fill=" + fitler_fill) if fitler_fill is not None else ""), True)
                    self.assertRest(metrics_period10 if fitler_fill is None else metrics_period10_fill,
                                    anode,
                                    "/rest/?scope=history&format=csv&print=pretty&period=10" +
                                    (("&method=" + filter_method) if filter_method is not None else "") +
                                    (("&fill=" + fitler_fill) if fitler_fill is not None else ""), True)
                    self.assertRest(metrics_period10 if fitler_fill is None else metrics_period5_fill,
                                    anode,
                                    "/rest/?scope=history&format=csv&print=pretty&period=5" +
                                    (("&method=" + filter_method) if filter_method is not None else "") +
                                    (("&fill=" + fitler_fill) if fitler_fill is not None else ""), True)
                    self.assertRest(metrics_period20 if fitler_fill is None else metrics_period20_fill,
                                    anode,
                                    "/rest/?scope=history&format=csv&print=pretty&period=20" +
                                    (("&method=" + filter_method) if filter_method is not None else "") +
                                    (("&fill=" + fitler_fill) if fitler_fill is not None else ""), True)

    def test_bounded(self):
        period = 1
        iterations = 101
        partition_index = 50
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_BOUNDED_TICKS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        metrics = self.assertRest(776, anode, "/rest/?scope=history", True)[1]
        for config in [
            FILE_CONFIG_FRONIUS_BOUNDED_TICKS,
            FILE_CONFIG_FRONIUS_BOUNDED_PARTITIONS
        ]:
            self.patch(sys, "argv", ["anode", "-c" + config, "-d" + DIR_ANODE, "-q"])
            anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
            self.assertEquals(0 if config == FILE_CONFIG_FRONIUS_BOUNDED_TICKS else 100,
                              self.assertRest(1,
                                              anode,
                                              "/rest/?metrics=anode.fronius.partitions&format=csv&print=pretty",
                                              True)[0]["Partitions (1 Second)"][0])
            self.assertEquals(TIME_START_OF_DAY + 2 + partition_index,
                              self.assertRest(partition_index,
                                              anode,
                                              "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                              True)[0]["bin_timestamp"][0])
            self.assertEquals(TIME_START_OF_DAY + iterations,
                              self.assertRest(partition_index,
                                              anode,
                                              "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                              True)[0]["bin_timestamp"][partition_index - 1])
            self.assertRest(metrics,
                            anode,
                            "/rest/?scope=history",
                            True)

    def test_unbounded(self):
        period = 1
        iterations = 100
        partition_index = 48
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_UNBOUNDED_LARGE, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        metrics = self.assertRest(1526, anode, "/rest/?scope=history", True)[1]
        for config in [
            FILE_CONFIG_FRONIUS_UNBOUNDED_DAY,
            FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL,
            FILE_CONFIG_FRONIUS_UNBOUNDED_LARGE
        ]:
            self.patch(sys, "argv", ["anode", "-c" + config, "-d" + DIR_ANODE, "-q"])
            anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
            self.assertEquals(0,
                              self.assertRest(1,
                                              anode,
                                              "/rest/?metrics=anode.fronius.partitions&format=csv&print=pretty",
                                              True)[0]["Partitions (1 Second)"][0])
            self.assertEquals(TIME_START_OF_DAY + 1,
                              self.assertRest(iterations,
                                              anode,
                                              "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                              True)[0]["bin_timestamp"][0])
            self.assertEquals(TIME_START_OF_DAY + 1 + partition_index,
                              self.assertRest(iterations,
                                              anode,
                                              "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                              True)[0]["bin_timestamp"][partition_index])
            self.assertRest(metrics,
                            anode,
                            "/rest/?scope=history",
                            True)

    def test_publish(self):
        period = 100
        self.patch(MqttPublishService, "isConnected", lambda myself: False)
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_PUBLISH, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=1)
        metrics = self.assertRest(41, anode, "/rest/?scope=publish&format=csv", True)[1]
        self.assertRest(metrics,
                        anode,
                        "/rest/?scope=publish&format=csv",
                        True)
        self.patch(MqttPublishService, "isConnected", lambda myself: True)
        self.clock_tick(anode, period, 10, True)
        self.assertRest(0,
                        anode,
                        "/rest/?scope=publish&format=csv",
                        True)

    def test_temporal(self):
        period = 1
        iterations = 3
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        self.assertRest(1,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=energy.export.grid&bins=1all-time&types=low",
                        True)
        self.assertRest(iterations,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=energy.export.grid&bins=1day&types=integral",
                        True)
        global test_repeats
        test_repeats = True
        self.clock_tick(anode, period, 60 * 60 * 24 + 1, True)
        self.assertRest(1,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=energy.export.grid&bins=1all-time&types=low",
                        True)
        self.assertRest(iterations + 1,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=energy.export.grid&bins=1day&types=integral",
                        True)

    def test_dailies(self):
        period = 1
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_UNBOUNDED_DAY, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=1)
        self.assertRest(2,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption.grid&bins=1all-time",
                        True)
        self.assertRest(0,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-morning.grid",
                        True)
        self.assertRest(0,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-evening.grid",
                        True)
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-morning.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-evening.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.clock_tick(anode, period, 60 * 60 * 2 - 1, True)
        self.assertRest(3,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption.grid&bins=1all-time",
                        True)
        self.assertRest(0,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-morning.grid",
                        True)
        self.assertRest(0,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-evening.grid",
                        True)
        self.assertEquals(1,
                          self.assertRest(2,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-morning.grid",
                                          True)[0]["Grid (1 Day)"][1])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-evening.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(1,
                          self.assertRest(2,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak.grid",
                                          True)[0]["Grid (1 Day)"][1])
        self.clock_tick(anode, period, 60 * 60 * 13, True)
        self.assertRest(4,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption.grid&bins=1all-time",
                        True)
        self.assertEquals(3,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-morning.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertRest(0,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-evening.grid",
                        True)
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-morning.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-evening.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.clock_tick(anode, period, 60 * 60 * 2, True)
        self.assertRest(5,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption.grid&bins=1all-time",
                        True)
        self.assertEquals(3,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-morning.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertRest(0,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-evening.grid",
                        True)
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-morning.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.assertEquals(1,
                          self.assertRest(2,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak.grid",
                                          True)[0]["Grid (1 Day)"][1])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-evening.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.clock_tick(anode, period, 60 * 60 * 4, True)
        self.assertRest(6,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption.grid&bins=1all-time",
                        True)
        self.assertEquals(3,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-morning.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertEquals(5,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-evening.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-morning.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.assertEquals(0,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-evening.grid",
                                          True)[0]["Grid (1 Day)"][0])
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.clock_tick(anode, period, 60 * 60 * 1, True)
        self.assertRest(7,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption.grid&bins=1all-time",
                        True)
        self.assertEquals(3,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-morning.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertEquals(5,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-evening.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-morning.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.assertEquals(2,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.assertEquals(1,
                          self.assertRest(2,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-evening.grid",
                                          True)[0]["Grid (1 Day)"][1])
        self.assertEquals(3,
                          self.assertRest(4,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak.grid",
                                          True)[0]["Grid (1 Day)"][3])
        self.clock_tick(anode, period, 60 * 60 * 2, True)
        self.assertRest(9,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption.grid&bins=1all-time",
                        True)
        self.assertEquals(3,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-morning.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertEquals(5,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-evening.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertEquals(0,
                          self.assertRest(4,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-morning.grid",
                                          True)[0]["Grid (1 Day)"][3])
        self.assertEquals(0,
                          self.assertRest(4,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak.grid",
                                          True)[0]["Grid (1 Day)"][3])
        self.assertEquals(0,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-evening.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.assertEquals(0,
                          self.assertRest(5,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak.grid",
                                          True)[0]["Grid (1 Day)"][4])
        self.clock_tick(anode, period, 60 * 60 * 1, True)
        self.assertRest(10,
                        anode,
                        "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption.grid&bins=1all-time",
                        True)
        self.assertEquals(3,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-morning.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertEquals(5,
                          self.assertRest(1,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak-evening.grid",
                                          True)[0]["Grid (All Time)"][0])
        self.assertEquals(1,
                          self.assertRest(5,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-morning.grid",
                                          True)[0]["Grid (1 Day)"][4])
        self.assertEquals(0,
                          self.assertRest(4,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-peak.grid",
                                          True)[0]["Grid (1 Day)"][3])
        self.assertEquals(0,
                          self.assertRest(3,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak-evening.grid",
                                          True)[0]["Grid (1 Day)"][2])
        self.assertEquals(1,
                          self.assertRest(6,
                                          anode,
                                          "/rest/?scope=history&print=pretty&format=csv&metrics=energy.consumption-off-peak.grid",
                                          True)[0]["Grid (1 Day)"][5])

    def test_state(self):
        period = 1
        iterations = 10
        iterations_repeat = 15
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        metrics = self.assertRest(1274, anode, "/rest/?scope=history", True)[1]
        self.assertTrue(metrics > 0)
        self.assertRest(metrics,
                        anode,
                        "/rest/?scope=history",
                        True)
        self.assertEquals(TIME_START_OF_DAY + iterations,
                          self.assertRest(iterations,
                                          anode,
                                          "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                          True)[0]["bin_timestamp"][iterations - 1])
        anode.store_state()
        self.assertRest(metrics,
                        anode,
                        "/rest/?scope=history",
                        True)
        self.assertEquals(TIME_START_OF_DAY + iterations,
                          self.assertRest(iterations,
                                          anode,
                                          "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                          True)[0]["bin_timestamp"][iterations - 1])
        anode.load_state()
        self.assertRest(metrics,
                        anode,
                        "/rest/?scope=history",
                        True)
        self.assertEquals(TIME_START_OF_DAY + iterations,
                          self.assertRest(iterations,
                                          anode,
                                          "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                          True)[0]["bin_timestamp"][iterations - 1])
        self.clock_tick(anode, 1, iterations_repeat)
        self.assertTrue(metrics <
                        self.assertRest(metrics,
                                        anode,
                                        "/rest/?scope=history",
                                        False))
        self.assertEquals(TIME_START_OF_DAY + iterations + iterations_repeat,
                          self.assertRest(iterations + iterations_repeat,
                                          anode,
                                          "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                          True)[0]["bin_timestamp"][iterations + iterations_repeat - 1])
        anode.load_state()
        self.assertRest(metrics,
                        anode,
                        "/rest/?scope=history",
                        True)
        self.assertEquals(TIME_START_OF_DAY + iterations,
                          self.assertRest(iterations,
                                          anode,
                                          "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                          True)[0]["bin_timestamp"][iterations - 1])
        self.setUp()
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_DB, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=(iterations * 11))
        metrics = self.assertRest(1676, anode, "/rest/?scope=history", True)[1]
        self.assertTrue(metrics > 0)
        self.assertRest(metrics,
                        anode,
                        "/rest/?scope=history",
                        True)
        anode.load_state()
        metrics_db = self.assertRest(1511, anode, "/rest/?scope=history", True)[1]
        self.assertTrue(metrics > metrics_db)
        self.assertRest(metrics_db,
                        anode,
                        "/rest/?scope=history",
                        False)
        self.clock_tick(anode, 1, iterations_repeat)
        self.assertTrue(metrics_db <
                        self.assertRest(0,
                                        anode,
                                        "/rest/?scope=history",
                                        False))
        anode.load_state()
        self.assertRest(metrics_db,
                        anode,
                        "/rest/?scope=history",
                        True)

    def test_partition(self):
        period = 1
        iterations = 10
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL_REPEAT_PARTITION, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        self.assertRest(iterations,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty&partitions=" +
                        str(iterations), True)
        self.assertRest(iterations,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty&partitions=" +
                        str(iterations / 2 + 1), True)
        self.assertRest(5,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty&partitions=" +
                        "3", True)
        self.assertRest(1,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty&partitions=" +
                        "1", True)
        self.assertRest(0,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty&partitions=" +
                        "0", True)
        self.assertRest(0,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty&partitions=" +
                        "-1", True)
        self.assertRest(0,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty&partitions=" +
                        "a", True)
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL_REPEAT_PARTITION, "-d" + DIR_ANODE_DB, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=1)
        self.clock_tick(anode, 1, 2 * 2460 * 60 + 1, True)
        self.assertRest(1,
                        anode,
                        "/rest/?metrics=power.consumption.grid&types=point&scope=history&format=csv&print=pretty&partitions=1",
                        True)

    def test_repeat(self):
        period = 10
        iterations = 5
        for config in [
            FILE_CONFIG_FRONIUS_REPEAT_NONE,
            FILE_CONFIG_FRONIUS_REPEAT_DAY,
            FILE_CONFIG_FRONIUS_REPEAT_PARTITION
        ]:
            self.patch(sys, "argv", ["anode", "-c" + config, "-d" + DIR_ANODE, "-q"])
            anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
            self.assertRest(iterations if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else iterations + 1,
                            anode,
                            "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty",
                            True)
            self.assertEquals(TIME_START_OF_DAY + 1,
                              self.assertRest(iterations if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else iterations + 1,
                                              anode,
                                              "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                              True)[0]["bin_timestamp"][0])
            self.assertEquals(TIME_START_OF_DAY + iterations * period - (0 if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else period),
                              self.assertRest(iterations if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else iterations + 1,
                                              anode,
                                              "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                              True)[0]["bin_timestamp"][iterations - 1])
            self.assertEquals(1,
                              self.assertRest(1,
                                              anode,
                                              "/rest/?metrics=power.export.grid&types=low&scope=history&format=csv&print=pretty",
                                              True)[0]["Grid (1 Day)"][0])
            self.assertEquals(4,
                              self.assertRest(iterations if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else iterations + 1,
                                              anode,
                                              "/rest/?metrics=energy.consumption.grid&types=integral&bins=1day&scope=history&format=csv&print=pretty",
                                              True)[0]["Grid (1 Day)"].iloc([[-1]])[-1])
            global test_repeats
            test_repeats = True
            self.clock_tick(anode, 1, 60 * 60 * 24 - iterations * period, skip_all=True)
            self.assertRest(iterations + (1 if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else 2),
                            anode,
                            "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty",
                            True)
            self.assertRest(1 if config == FILE_CONFIG_FRONIUS_REPEAT_NONE else 2,
                            anode,
                            "/rest/?metrics=power.export.grid&types=low&scope=history&format=csv&print=pretty",
                            True)
            self.assertEquals(0,
                              self.assertRest(iterations + (1 if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else 2),
                                              anode,
                                              "/rest/?metrics=energy.consumption.grid&types=integral&bins=1day&scope=history&format=csv&print=pretty",
                                              True)[0]["Grid (1 Day)"].iloc([[-1]])[-1])
            self.clock_tick(anode, 1, 1, skip_all=True)
            self.assertRest(iterations + (1 if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else 2),
                            anode,
                            "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty",
                            True)
            self.assertRest(1 if config == FILE_CONFIG_FRONIUS_REPEAT_NONE else 2,
                            anode,
                            "/rest/?metrics=power.export.grid&types=low&scope=history&format=csv&print=pretty",
                            True)
            self.assertEquals(0,
                              self.assertRest(iterations + (1 if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else 2),
                                              anode,
                                              "/rest/?metrics=energy.consumption.grid&types=integral&bins=1day&scope=history&format=csv&print=pretty",
                                              True)[0]["Grid (1 Day)"].iloc([[-1]])[-1])
            self.clock_tick(anode, 1, period - 1, skip_all=True)
            self.assertRest(iterations + (1 if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else 3),
                            anode,
                            "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty",
                            True)
            self.assertRest(1 if config == FILE_CONFIG_FRONIUS_REPEAT_NONE else 2,
                            anode,
                            "/rest/?metrics=power.export.grid&types=low&scope=history&format=csv&print=pretty",
                            True)
            self.assertEquals(0,
                              self.assertRest(iterations + (1 if config != FILE_CONFIG_FRONIUS_REPEAT_DAY else 3),
                                              anode,
                                              "/rest/?metrics=energy.consumption.grid&types=integral&bins=1day&scope=history&format=csv&print=pretty",
                                              True)[0]["Grid (1 Day)"].iloc([[-1]])[-1])

    def test_filter(self):
        period = 1
        iterations = 10
        for config in [
            FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL,
            FILE_CONFIG_FRONIUS_UNBOUNDED_DAY,
            FILE_CONFIG_FRONIUS_UNBOUNDED_LARGE
        ]:
            self.patch(sys, "argv", ["anode", "-c" + config, "-d" + DIR_ANODE, "-q"])
            anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
            self.assertRest(iterations,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&start=" + str(TIME_START_OF_DAY - 1), True)
            self.assertRest(iterations,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&finish=" + str(TIME_START_OF_DAY + period * iterations), True)
            self.assertRest(iterations,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&start=" + str(TIME_START_OF_DAY - 1) +
                            "&finish=" + str(TIME_START_OF_DAY + period * iterations), True)
            self.assertRest(iterations - 5,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&start=" + str(TIME_START_OF_DAY + period * 6), True)
            self.assertRest(iterations - 5,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&finish=" + str(TIME_START_OF_DAY + period * (iterations - 5)), True)
            self.assertRest(iterations - 10,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&start=" + str(TIME_START_OF_DAY + period * 6) +
                            "&finish=" + str(TIME_START_OF_DAY + period * (iterations - 5)), True)
            self.assertRest(1,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&start=" + str(TIME_START_OF_DAY + period * (iterations - 5)) +
                            "&finish=" + str(TIME_START_OF_DAY + period * (iterations - 5)), True)
            self.assertRest(0,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&start=" + str(TIME_START_OF_DAY + period * iterations + 1), True)
            self.assertRest(0,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&finish=" + str(TIME_START_OF_DAY - 1), True)
            self.assertRest(0,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&start=" + str(TIME_START_OF_DAY - 1) +
                            "&finish=" + str(TIME_START_OF_DAY - 1), True)
            self.assertRest(0,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&finish=" + str(TIME_START_OF_DAY + period * iterations) +
                            "&start=" + str(TIME_START_OF_DAY + period * iterations + 1), True)
            self.assertRest(0,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&start=" + str(TIME_START_OF_DAY + period * iterations + 1) +
                            "&finish=" + str(TIME_START_OF_DAY - 1), True)
            self.assertRest(0,
                            anode,
                            "/rest/?metrics=power.production.inverter&types=point&scope=history&format=csv&print=pretty" +
                            "&finish=" + str(TIME_START_OF_DAY - 1) +
                            "&start=" + str(TIME_START_OF_DAY + period * iterations + 1), True)

    def test_bad_plots(self):
        period = 1
        iterations = 50
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty&types=point",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty&types=point&partition=100",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty&types=point&partition=2",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty&types=point&partition=1",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty&types=point&partition=1&period=10",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty&types=point&partition=1&period=10&method=min",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty&types=point&partition=1&period=10&method=max&fill=zeros",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty&types=point&partition=1&period=10&method=max&fill=linear",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history&print=pretty",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power&scope=history",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg&metrics=power",
                        False, True)
        self.assertRest(0,
                        anode,
                        "/rest/?format=svg",
                        False, True)

    # TODO: Re-enable once this does not hang when run with old pkl'd data
    # def test_good_plots(self):
    #     self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS_RUN, "-d" + DIR_ANODE_DB, "-q"])
    #     anode = self.anode_init(False, False, False, False, period=1, iterations=0)
    #     last_timestamp = self.assertRest(0, anode, "/rest/?&metrics=temperature.outdoor.roof&scope=history&format=csv",
    #                                      False)[0]["bin_timestamp"].iloc[-1]
    #     for parameters in [
    #         ("&start=" + str(last_timestamp + 1) + "&period=1&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - 1) + "&period=1&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - 10) + "&period=1&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - 60) + "&period=5&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - 60 * 2) + "&period=5&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 - 100)) + "&period=5&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60)) + "&period=5&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 4)) + "&period=300&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 8)) + "&period=300&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 16)) + "&period=300&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24)) + "&period=300&method=max&fill=linear"),
    #         "&partitions=1&period=300&method=max&fill=linear",
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 2 + 60 * 60 * 4)) + "&period=1800&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 2 + 60 * 60 * 8)) + "&period=1800&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 2 + 60 * 60 * 16)) + "&period=1800&method=max&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 2 + 60 * 60 * 24)) + "&period=1800&method=max&fill=linear"),
    #         "&partitions=3&period=1800&method=max&fill=linear",
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 6 + 60 * 60 * 4)) + "&period=3600&method=median&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 6 + 60 * 60 * 8)) + "&period=3600&method=median&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 6 + 60 * 60 * 16)) + "&period=3600&method=median&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 6 + 60 * 60 * 24)) + "&period=3600&method=median&fill=linear"),
    #         "&partitions=7&period=3600&method=median&fill=linear",
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 13 + 60 * 60 * 4)) + "&period=7200&method=median&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 13 + 60 * 60 * 8)) + "&period=7200&method=median&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 13 + 60 * 60 * 16)) + "&period=7200&method=median&fill=linear"),
    #         ("&start=" + str(last_timestamp - (60 * 60 * 24 * 13 + 60 * 60 * 24)) + "&period=7200&method=median&fill=linear"),
    #         "&period=7200&method=median&fill=linear"
    #     ]:
    #         self.assertRest(0,
    #                         anode,
    #                         "/rest/?metrics=temperature&bins=1day&bins=50s&bins=2s&scope=history&print=pretty&types=point&units=°C&format=svg" + parameters,
    #                         False, True)
    #         self.assertRest(0,
    #                         anode,
    #                         "/rest/?metrics=power&scope=history&print=pretty&types=point&units=W&format=svg" + parameters,
    #                         False, True)

    def test_oneoff(self):
        period = 1
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=1)
        self.assertRest(0,
                        anode,
                        "/rest/?units=°&print=pretty",
                        False, True)


# noinspection PyPep8Naming,PyStatementEffect,PyUnusedLocal
class MockRequest:
    def __init__(self, uri):
        self.uri = uri

    @staticmethod
    def setHeader(name, value):
        None


# noinspection PyPep8Naming,PyUnusedLocal
class MockHttpResponse:
    def __init__(self, url):
        self.url = url
        self.code = 200 if url in HTTP_GETS else 404

    def addCallbacks(self, callback, errback=None):
        callback(self)


# noinspection PyPep8, PyPep8Naming, PyUnusedLocal
class MockHttpResponseContent:
    def __init__(self, response):
        self.response = response
        self.content = ANodeTest.template_populate(HTTP_GETS[response.url]) if response.code == 200 else HTTP_GETS["http_404"]

    def addCallbacks(self, callback, errback=None):
        callback(self.content)


# noinspection PyPep8Naming,PyUnusedLocal
class MockDeferToThread:
    def __init__(self, to_execute, *arguments, **keyword_arguments):
        self.function = to_execute
        self.arguments = arguments
        self.keyword_arguments = keyword_arguments

    def addCallback(self, callback, *arguments, **keyword_arguments):
        callback(self.function(*self.arguments, **self.keyword_arguments), *arguments, **keyword_arguments)


test_ticks = 0
test_clock = None
test_svg_counter = -1
test_randomise = False
test_repeats = True
test_nulls = False
test_corruptions = False

TEMPLATE_INTEGER_MAX = 8

TIME_BOOT = calendar.timegm(time.gmtime())
TIME_BOOT_LOCAL = time.localtime()
TIME_OFFSET = calendar.timegm(TIME_BOOT_LOCAL) - calendar.timegm(time.gmtime(time.mktime(TIME_BOOT_LOCAL)))
TIME_START_OF_DAY = (TIME_BOOT + TIME_OFFSET) - (TIME_BOOT + TIME_OFFSET) % (24 * 60 * 60) - TIME_OFFSET

DIR_ROOT = os.path.dirname(__file__) + "/../../"
DIR_TARGET = (DIR_ROOT + "../../../target/") if os.path.isdir(DIR_ROOT + "../../../target/") else (DIR_ROOT + "../../target/")
DIR_ANODE = DIR_TARGET + "anode-runtime/"
DIR_ANODE_TEST = DIR_TARGET + "anode-tests/"
DIR_ANODE_DB = DIR_ROOT + "anode/test/pickle"

FILE_CONFIG_BARE = DIR_ROOT + "anode/test/config/anode_bare.yaml"
FILE_CONFIG_PLUGINS = DIR_ROOT + "anode/test/config/anode_plugins.yaml"
FILE_CONFIG_PLUGINS_RUN = DIR_ROOT + "anode/test/config/anode_plugins_run.yaml"
FILE_CONFIG_FRONIUS_DB = DIR_ROOT + "anode/test/config/anode_fronius_db.yaml"
FILE_CONFIG_FRONIUS_PUBLISH = DIR_ROOT + "anode/test/config/anode_fronius_publish.yaml"
FILE_CONFIG_FRONIUS_REPEAT_NONE = DIR_ROOT + "anode/test/config/anode_fronius_repeat_none.yaml"
FILE_CONFIG_FRONIUS_REPEAT_DAY = DIR_ROOT + "anode/test/config/anode_fronius_repeat_day.yaml"
FILE_CONFIG_FRONIUS_REPEAT_PARTITION = DIR_ROOT + "anode/test/config/anode_fronius_repeat_partition.yaml"
FILE_CONFIG_FRONIUS_BOUNDED_TICKS = DIR_ROOT + "anode/test/config/anode_fronius_bounded_ticks.yaml"
FILE_CONFIG_FRONIUS_BOUNDED_PARTITIONS = DIR_ROOT + "anode/test/config/anode_fronius_bounded_partitions.yaml"
FILE_CONFIG_FRONIUS_UNBOUNDED_DAY = DIR_ROOT + "anode/test/config/anode_fronius_unbounded_day.yaml"
FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL = DIR_ROOT + "anode/test/config/anode_fronius_unbounded_small.yaml"
FILE_CONFIG_FRONIUS_UNBOUNDED_LARGE = DIR_ROOT + "anode/test/config/anode_fronius_unbounded_large.yaml"
FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL_REPEAT_PARTITION = DIR_ROOT + "anode/test/config/anode_fronius_unbounded_small_repeat_partition.yaml"

FILE_SVG_HTML = DIR_ROOT + "anode/test/web/index.html"

HTTP_POSTS = {
    "davis":
        ilio.read(DIR_ROOT + "anode/test/template/web_davis_record_packet_template.json")
}

# noinspection PyPep8
HTTP_GETS = {
    "http_404":
        u"""<html><body>HTTP 404</body></html>""",
    "https://api.netatmo.com/oauth2/token":
        ilio.read(DIR_ROOT + "anode/test/template/web_netatmo_token_template.json"),
    "https://api.netatmo.com/api/devicelist":
        ilio.read(DIR_ROOT + "anode/test/template/web_netatmo_weather_template.json"),
    "http://10.0.1.203/solar_api/v1/GetPowerFlowRealtimeData.fcgi":
        ilio.read(DIR_ROOT + "anode/test/template/web_fronius_flow_template.json"),
    "http://10.0.1.203/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System":
        ilio.read(DIR_ROOT + "anode/test/template/web_fronius_meter_template.json"),
    "http://api.wunderground.com/api/8539276b98b4973b/forecast10day/q/zmw:00000.6.94615.json":
        ilio.read(DIR_ROOT + "anode/test/template/web_wunderground_10dayforecast_template.json")
}
