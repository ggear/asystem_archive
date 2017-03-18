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
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase

from anode.anode import main


# noinspection PyPep8Naming, PyUnresolvedReferences, PyShadowingNames,PyPep8,PyTypeChecker
class ANodeTest(TestCase):
    def setUp(self):
        self.patch(treq, "get", lambda url, timeout=0, pool=None: MockHttpResponse(url))
        self.patch(treq, "post", lambda url, data, timeout=0, pool=None: MockHttpResponse(url))
        self.patch(treq, "text_content", lambda response: MockHttpResponseContent(response))
        self.patch(threads, "deferToThread", lambda function, *arguments, **keyword_arguments: function(*arguments, **keyword_arguments))
        shutil.rmtree(DIR_ANODE, ignore_errors=True)
        os.makedirs(DIR_ANODE)
        shutil.rmtree(DIR_ANODE_TEST, ignore_errors=True)
        os.makedirs(DIR_ANODE_TEST)
        print("")

    @staticmethod
    def clock_tick(anode, period, periods):
        global test_ticks
        ANodeTest.clock_tock(anode)
        for tickTock in range(0, period * periods, period):
            test_clock.advance(period)
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
                   not response_df_column.startswith("Time")), response_text

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
                                    "/rest/?metrics=rainrate&types=mean&units=mm" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 1,
                                    anode,
                                    "/rest/?metrics=rainrate&types=mean&units=mm/h" +
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
                                    "/rest/?metrics=windgustbearing.outdoor.roof&units=°" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 14,
                                    anode,
                                    "/rest/?metrics=energy&print=pretty" +
                                    (("&format=" + filter_format) if filter_format is not None else "") +
                                    (("&scope=" + filter_scope) if filter_scope is not None else ""), True)
                    self.assertRest(0 if filter_scope == "publish" else 17,
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
        period = 10
        iterations = 25
        metrics_period10 = 323
        metrics_period10_fill = 910
        metrics_period5_fill = 1785
        metrics_period20 = 179
        metrics_period20_fill = 455
        for config in [
            FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL,
            FILE_CONFIG_FRONIUS_UNBOUNDED_LARGE
        ]:
            self.patch(sys, "argv", ["anode", "-c" + config, "-d" + DIR_ANODE, "-q"])
            anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
            for filter_method in [None, "min", "max"]:
                for fitler_fill in [None, "zeros", "linear"]:
                    self.assertRest(metrics_period10 if fitler_fill is None else metrics_period10_fill,
                                    anode,
                                    "/rest/?scope=history&format=csv&print=pretty" +
                                    (("&method=" + filter_method) if filter_method is not None else "") +
                                    (("&fill=" + fitler_fill) if fitler_fill is not None else ""), True)
                    self.assertRest(metrics_period10 if fitler_fill is None else metrics_period10_fill,
                                    anode,
                                    "/rest/?scope=history&format=csv&print=pretty&period=" + str(period) +
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
        iterations = 100
        partition_index = 48
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_BOUNDED_TICKS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        metrics = self.assertRest(611, anode, "/rest/?scope=history", True)[1]
        for config in [
            FILE_CONFIG_FRONIUS_BOUNDED_TICKS,
            FILE_CONFIG_FRONIUS_BOUNDED_PARTITIONS
        ]:
            self.patch(sys, "argv", ["anode", "-c" + config, "-d" + DIR_ANODE, "-q"])
            anode = self.anode_init(False, False, False, False, period=period, iterations=100)
            self.assertEquals(0 if config == FILE_CONFIG_FRONIUS_BOUNDED_TICKS else 100,
                              self.assertRest(1,
                                              anode,
                                              "/rest/?metrics=anode.fronius.partitions&format=csv&print=pretty",
                                              True)[0]["Partitions"][0])
            self.assertEquals(TIME_START_OF_DAY + 4 + partition_index,
                              self.assertRest(1 + partition_index,
                                              anode,
                                              "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                              True)[0]["bin_timestamp"][0])
            self.assertEquals(TIME_START_OF_DAY + 100,
                              self.assertRest(1 + partition_index,
                                              anode,
                                              "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                              True)[0]["bin_timestamp"][partition_index])
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
        metrics = self.assertRest(1223, anode, "/rest/?scope=history", True)[1]
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
                                              True)[0]["Partitions"][0])
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

    def test_temporal(self):
        period = 1
        iterations = 3
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        self.assertRest(1,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=energy.export.grid&bins=1alltime&types=low",
                        True)
        self.assertRest(iterations,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=energy.export.grid&bins=1day&types=integral",
                        True)
        global test_repeats
        test_repeats = True
        test_clock.advance(60 * 60 * 24 + 1)
        self.assertRest(1,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=energy.export.grid&bins=1alltime&types=low",
                        True)
        self.assertRest(iterations + 1,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=energy.export.grid&bins=1day&types=integral",
                        True)

    def test_state(self):
        period = 1
        iterations = 10
        iterations_repeat = 15
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        metrics = self.assertRest(1253, anode, "/rest/?scope=history", True)[1]
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
                        self.assertRest(2973,
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
        metrics = self.assertRest(1343, anode, "/rest/?scope=history", True)[1]
        self.assertTrue(metrics > 0)
        self.assertRest(metrics,
                        anode,
                        "/rest/?scope=history",
                        True)
        anode.load_state()
        metrics_db = self.assertRest(1211, anode, "/rest/?scope=history", True)[1]
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
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL, "-d" + DIR_ANODE, "-q"])
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

    def test_repeat(self):
        period = 1
        iterations = 5
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_FRONIUS_REPEAT, "-d" + DIR_ANODE, "-q"])
        anode = self.anode_init(False, False, False, False, period=period, iterations=iterations)
        self.assertRest(iterations,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty",
                        True)
        self.assertEquals(TIME_START_OF_DAY + 1,
                          self.assertRest(iterations,
                                          anode,
                                          "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                          True)[0]["bin_timestamp"][0])
        self.assertEquals(TIME_START_OF_DAY + iterations,
                          self.assertRest(iterations,
                                          anode,
                                          "/rest/?scope=history&format=csv&metrics=energy.export.grid&bins=1day&types=integral",
                                          True)[0]["bin_timestamp"][iterations - 1])
        self.assertRest(1,
                        anode,
                        "/rest/?metrics=power.export.grid&types=low&scope=history&format=csv&print=pretty",
                        True)
        global test_repeats
        test_repeats = True
        test_clock.advance(60 * 60 * 24 + 1)
        self.assertRest(iterations + 1,
                        anode,
                        "/rest/?metrics=power.export.grid&types=point&scope=history&format=csv&print=pretty",
                        True)
        self.assertRest(2,
                        anode,
                        "/rest/?metrics=power.export.grid&types=low&scope=history&format=csv&print=pretty",
                        True)

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

    def test_good_plots(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS_RUN, "-d" + DIR_ANODE_DB, "-q"])
        anode = self.anode_init(False, False, False, False, period=1, iterations=0)
        anode.load_state()
        last_timestamp = self.assertRest(0, anode, "/rest/?&metrics=temperature.outdoor.apparent&scope=history&format=csv",
                                         False)[0]["bin_timestamp"].iloc[-1]
        for parameters in [
            ("&start=" + str(last_timestamp + 1) + "&period=1&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - 1) + "&period=1&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - 10) + "&period=1&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - 60) + "&period=5&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - 60 * 2) + "&period=5&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 - 100)) + "&period=5&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60)) + "&period=5&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 4)) + "&period=300&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 8)) + "&period=300&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 16)) + "&period=300&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24)) + "&period=300&method=max&fill=linear"),
            "&partitions=1&period=300&method=max&fill=linear",
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 2 + 60 * 60 * 4)) + "&period=1800&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 2 + 60 * 60 * 8)) + "&period=1800&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 2 + 60 * 60 * 16)) + "&period=1800&method=max&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 2 + 60 * 60 * 24)) + "&period=1800&method=max&fill=linear"),
            "&partitions=3&period=1800&method=max&fill=linear",
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 6 + 60 * 60 * 4)) + "&period=3600&method=median&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 6 + 60 * 60 * 8)) + "&period=3600&method=median&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 6 + 60 * 60 * 16)) + "&period=3600&method=median&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 6 + 60 * 60 * 24)) + "&period=3600&method=median&fill=linear"),
            "&partitions=7&period=3600&method=median&fill=linear",
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 13 + 60 * 60 * 4)) + "&period=7200&method=median&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 13 + 60 * 60 * 8)) + "&period=7200&method=median&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 13 + 60 * 60 * 16)) + "&period=7200&method=median&fill=linear"),
            ("&start=" + str(last_timestamp - (60 * 60 * 24 * 13 + 60 * 60 * 24)) + "&period=7200&method=median&fill=linear"),
            "&period=7200&method=median&fill=linear"
        ]:
            self.assertRest(0,
                            anode,
                            "/rest/?metrics=temperature&bins=1day&bins=50s&bins=2s&scope=history&print=pretty&types=point&units=°C&format=svg" + parameters,
                            False, True)
            self.assertRest(0,
                            anode,
                            "/rest/?metrics=power&scope=history&print=pretty&types=point&units=W&format=svg" + parameters,
                            False, True)

    def test_oneoff(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_PLUGINS, "-q"])
        anode = self.anode_init(False, False, False, False, period=1, iterations=10)
        self.assertRest(0,
                        anode,
                        "/rest/?scope=history&format=csv&print=pretty&metrics=anode.fronius.partitions",
                        False, True)


# TODO: Refactor memory/serialised profiling into main line
# class ANodeModelTest(TestCase):
#     def __init__(self, *args, **kwargs):
#         super(ANodeModelTest, self).__init__(*args, **kwargs)
#         with gzip.open(FILE_DATUMS_CSV, 'rt') as file_csv:
#             self.datums_dict = Plugin.datums_csv_to_dict(csv.DictReader(file_csv))
#         self.datums_df = {"df": Plugin.datums_dict_to_df(self.datums_dict["dict"])}
#
#     def test_size(self):
#         datum_sizes_tmp = [["model", "format", "container", "memory", "disk"]]
#
#         def datum_sizes_add(datums, datums_count, datums_model, datums_format, datums_container):
#             size_disk = None
#             if datums_model == "dict":
#                 datums = {datums_model: datums} if datums_container is not None else datums
#                 size_memory = sum(sys.getsizeof(datum) for datum in datums[datums_model]) + \
#                               sum(sum(map(sys.getsizeof, datum.itervalues())) for datum in datums[datums_model]) + \
#                               8 * len(datums[datums_model]) * len(DATUM_SCHEMA_JSON[7]["fields"]) + \
#                               (0 if datums_container is None else sys.getsizeof(datums[datums_model]))
#             else:
#                 datums = {datums_model: [datums]} if datums_container is not None else datums
#                 if datums_model == "df":
#                     if datums_container is None:
#                         size_memory = sum(sys.getsizeof(datum) for datum in datums[datums_model]) + \
#                                       sum(sys.getsizeof(datum["data_df"]) for datum in datums[datums_model]) + \
#                                       8 * len(datums[datums_model]) * (len(DATUM_SCHEMA_JSON[7]["fields"]) - 3)
#                     else:
#                         size_memory = sum(sys.getsizeof(datum) for datum in datums[datums_model]) + \
#                                       sum(sys.getsizeof(datum_df) for datum in datums[datums_model] for datum_df in datum)
#                 else:
#                     size_memory = sum(sys.getsizeof(datum) for datum in datums[datums_model])
#                     with tempfile.NamedTemporaryFile() as datums_file:
#                         for datum in datums[datums_model]:
#                             datums_file.write(datum)
#                         datums_file.flush()
#                         size_disk = os.path.getsize(datums_file.name)
#             datum_sizes_tmp.append([datums_model, datums_format, datums_container, size_memory / datums_count,
#                                     size_disk / datums_count if size_disk is not None else None])
#
#         datums_dict_count = len(self.datums_dict["dict"])
#         for datum_format in [("dict", "long", None, self.datums_dict), ("df", "wide (int64)", None, self.datums_df),
#                              ("avro", "long", None, self.datums_dict),
#                              ("csv", "long", None, self.datums_dict), ("json", "long", None, self.datums_dict)]:
#             datum_sizes_add(Plugin.datum_to_format(datum_format[3], datum_format[0], {}), datums_dict_count, datum_format[0], datum_format[1],
#                             datum_format[2])
#         for datum_format in [("dict", "long", "list", self.datums_dict), ("df", "wide (Decimal)", "list", self.datums_df),
#                              ("csv", "wide", "csv", self.datums_df), ("json", "long", "json", self.datums_dict)]:
#             datum_sizes_add(Plugin.datums_to_format(datum_format[3], datum_format[0], {}), datums_dict_count, datum_format[0], datum_format[1],
#                             datum_format[2])
#         print("Datum sizes:\n" + tabulate(datum_sizes_tmp, tablefmt='grid'))


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
    def __init__(self, function, *arguments, **keyword_arguments):
        self.function = function
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
DIR_ANODE_DB = DIR_ROOT + "anode/test/data/"

FILE_CONFIG_BARE = DIR_ROOT + "anode/test/data/anode_bare.yaml"
FILE_CONFIG_PLUGINS = DIR_ROOT + "anode/test/data/anode_plugins.yaml"
FILE_CONFIG_PLUGINS_RUN = DIR_ROOT + "anode/test/data/anode_plugins_run.yaml"
FILE_CONFIG_FRONIUS_DB = DIR_ROOT + "anode/test/data/anode_fronius_db.yaml"
FILE_CONFIG_FRONIUS_REPEAT = DIR_ROOT + "anode/test/data/anode_fronius_repeat.yaml"
FILE_CONFIG_FRONIUS_BOUNDED_TICKS = DIR_ROOT + "anode/test/data/anode_fronius_bounded_ticks.yaml"
FILE_CONFIG_FRONIUS_BOUNDED_PARTITIONS = DIR_ROOT + "anode/test/data/anode_fronius_bounded_partitions.yaml"
FILE_CONFIG_FRONIUS_UNBOUNDED_DAY = DIR_ROOT + "anode/test/data/anode_fronius_unbounded_day.yaml"
FILE_CONFIG_FRONIUS_UNBOUNDED_SMALL = DIR_ROOT + "anode/test/data/anode_fronius_unbounded_small.yaml"
FILE_CONFIG_FRONIUS_UNBOUNDED_LARGE = DIR_ROOT + "anode/test/data/anode_fronius_unbounded_large.yaml"

FILE_SVG_HTML = DIR_ROOT + "anode/test/web/index.html"

FILE_DATUMS_CSV = DIR_ROOT + "anode/test/data/datums_power_production_grid_inverter_point.csv.gz"

HTTP_POSTS = {
    "davis":
        ilio.read(DIR_ROOT + "anode/test/data/web_davis_record_packet_template.json")
}

# noinspection PyPep8
HTTP_GETS = {
    "http_404":
        u"""<html><body>HTTP 404</body></html>""",
    "https://api.netatmo.com/oauth2/token":
        ilio.read(DIR_ROOT + "anode/test/data/web_netatmo_token_template.json"),
    "https://api.netatmo.com/api/devicelist":
        ilio.read(DIR_ROOT + "anode/test/data/web_netatmo_weather_template.json"),
    "http://10.0.1.203/solar_api/v1/GetPowerFlowRealtimeData.fcgi":
        ilio.read(DIR_ROOT + "anode/test/data/web_fronius_flow_template.json"),
    "http://10.0.1.203/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System":
        ilio.read(DIR_ROOT + "anode/test/data/web_fronius_meter_template.json"),
    "http://api.wunderground.com/api/8539276b98b4973b/forecast10day/q/zmw:00000.6.94615.json":
        ilio.read(DIR_ROOT + "anode/test/data/web_wunderground_10dayforecast_template.json")
}
