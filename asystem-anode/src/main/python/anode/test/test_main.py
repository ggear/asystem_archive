# -*- coding: utf-8 -*-

from __future__ import print_function

import calendar
import csv
import datetime
import gzip
import json
import os.path
import sys
import tempfile
import time
import urlparse
from StringIO import StringIO
from random import randint

import ilio
import pandas
import treq
from tabulate import tabulate
from twisted.internet import threads
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase

from anode.anode import main
from anode.plugin.plugin import DATUM_SCHEMA_JSON
from anode.plugin.plugin import Plugin


# noinspection PyPep8Naming, PyUnresolvedReferences, PyShadowingNames
class ANodeTest(TestCase):
    def setUp(self):
        global test_ticks
        global test_clock
        test_ticks = 0
        test_clock = Clock()
        self.patch(treq, "get", lambda url, timeout=0, pool=None: MockHttpResponse(url))
        self.patch(treq, "post", lambda url, data, timeout=0, pool=None: MockHttpResponse(url))
        self.patch(treq, "text_content", lambda response: MockHttpResponseContent(response))
        self.patch(threads, "deferToThread", lambda function, *arguments, **keyword_arguments: function(*arguments, **keyword_arguments))
        print("")

    @staticmethod
    def clock_tick(periods):
        global test_ticks
        for tickTock in range(0, TIME_TICK_PERIOD * periods, TIME_TICK_PERIOD):
            test_clock.advance(TIME_TICK_PERIOD)
            test_ticks += 1

    @staticmethod
    def clock_tock(anode):
        for i in range(test_ticks + 1):
            for source in HTTP_POSTS:
                anode.put_datums({"sources": [source]}, ANodeTest.substitute(HTTP_POSTS[source], counter=i))

    @staticmethod
    def substitute(template, counter=None):
        epoch = (int(test_clock.seconds()) if counter is None else (counter * TIME_TICK_PERIOD)) + TIME_BOOT
        if test_nulls:
            integer = "null"
            epoch = "null"
        elif test_randomise:
            integer = randint(0, TEMPLATE_INTEGER_MAX - 1)
        else:
            integer = (test_ticks if counter is None else counter) % TEMPLATE_INTEGER_MAX
        if test_repeats:
            if integer < 2:
                integer = 1
                epoch = 1485179384
        else:
            if integer == 0:
                integer = TEMPLATE_INTEGER_MAX
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
        response_json = json.loads(ANodeTest.rest(anode, url))
        return response_json, len(response_json)

    @staticmethod
    def rest_csv(anode, url):
        response_text = ANodeTest.rest(anode, url)
        response_df = pandas.read_csv(StringIO(response_text)) if len(response_text) > 0 else pandas.DataFrame()
        return response_df, \
               sum(response_df[response_df_column].count() for response_df_column in response_df if response_df_column.startswith("data_value"))

    def assertRest(self, assertion, anode, url, iterations=1, log=False):
        response = None
        response_format = urlparse.parse_qs(urlparse.urlparse(url).query)["format"][0] \
            if "format" in urlparse.parse_qs(urlparse.urlparse(url).query) else "json"
        if response_format == "json":
            response = self.rest_json(anode, url)
        elif response_format == "csv":
            response = self.rest_csv(anode, url)
        else:
            raise Exception("Unknown format [{}]".format(response_format))
        if log or (iterations > 0 and assertion != response[1]):
            print("RESTful [{}] {} response:\n{}"
                  .format(url, response_format.upper(),
                          json.dumps(response[0], sort_keys=True, indent=4, separators=(',', ': ')) if response_format == "json" else response[0]))
            print("RESTful [{}] {} response includes [{}] datums".format(url, response_format.upper(), response[1]))
        if iterations > 0:
            self.assertEquals(assertion, iterations * response[1])
        return response

    def assert_anode(self, iterations=-1, radomise=False, repeats=False, nulls=False, corruptions=False, callback=None, url=None):
        global test_ticks
        global test_randomise
        global test_repeats
        global test_nulls
        global test_corruptions
        test_ticks = 0
        test_randomise = radomise
        test_repeats = repeats
        test_nulls = nulls
        test_corruptions = corruptions
        anode = main(test_clock, callback)
        self.clock_tock(anode)
        self.assertTrue(anode is not None)
        assert_results = []
        if url is not None:
            assert_results.append(self.assertRest(-1, anode, url, -1, True))
        else:
            metrics = 0
            metrics_anode = 0
            for metric in self.rest_json(anode, "/rest/?metrics=anode")[0]:
                metrics_anode += 1
                if metric["data_metric"].endswith("metrics"):
                    metrics += metric["data_value"]
            for scope in [None, "last", "publish", "history"]:
                for format in [None, "json", "csv"]:
                    assert_results.append(self.assertRest(0, anode,
                                                          "/rest/?metrics=some.fake.metric" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0, anode,
                                                          "/rest/?metrics=some.fake.metric&metrics=some.other.fake.metric" + (
                                                              ("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0, anode,
                                                          "/rest/?metrics=power&types=fake.type" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0, anode,
                                                          "/rest/?metrics=power&units=°" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0, anode,
                                                          "/rest/?metrics=power&types=point&bins=fake_bin" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 1, anode,
                                                          "/rest/?metrics=power.production.inverter&bins=1second" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 1, anode,
                                                          "/rest/?metrics=power.production.inverter&types=point" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 1, anode,
                                                          "/rest/?metrics=power.production.inverter&types=point&bins=1second" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 1, anode,
                                                          "/rest/?metrics=power.production.inverter&metrics=&types=point&bins=1second" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 1, anode,
                                                          "/rest/?metrics=power.production.inverter&metrics=some.fake.metric&metrics=&types=point&bins=1second" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 1, anode,
                                                          "/rest/?metrics=power.production.inverter&metrics=&types=point&types=fake.type&bins=1second" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 1, anode,
                                                          "/rest/?metrics=power.production.inverter&metrics=&types=point&bins=1second&bins=fake_bin" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 1, anode,
                                                          "/rest/?metrics=power.production.inverter&metrics=&types=point&bins=1second&units=W" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 2, anode,
                                                          "/rest/?metrics=power.production.inverter&metrics=power.production.grid&metrics=&types=point&bins=1second" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 3, anode,
                                                          "/rest/?metrics=power.production.inverter" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 3, anode,
                                                          "/rest/?metrics=windgustbearing.outdoor.roof&units=°" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 17, anode,
                                                          "/rest/?bins=2second" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(self.assertRest(0 if (nulls or corruptions or scope == "publish") else 21, anode,
                                                          "/rest/?metrics=power" +
                                                          (("&format=" + format) if format is not None else "") +
                                                          (("&scope=" + scope) if scope is not None else ""),
                                                          iterations))
                    assert_results.append(
                        self.assertRest(0 if (scope == "publish") else (metrics if scope != "history" else (metrics - metrics_anode)), anode,
                                        "/rest/?something=else" +
                                        (("&format=" + format) if format is not None else "") +
                                        (("&scope=" + scope) if scope is not None else ""), iterations))
                    assert_results.append(
                        self.assertRest(0 if (scope == "publish") else (metrics if scope != "history" else (metrics - metrics_anode)), anode,
                                        "/rest/?" +
                                        (("&format=" + format) if format is not None else "") +
                                        (("&scope=" + scope) if scope is not None else ""), iterations))
        return assert_results

    def test_main_default(self):
        self.patch(sys, "argv", ["anode", "--config=" + FILE_CONFIG])
        self.assert_anode(1, False, True, False, False, lambda: self.clock_tick(1))

    def test_main_verbose_short(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG, "-v"])
        self.assert_anode(1, False, True, False, False, lambda: self.clock_tick(1))

    def test_main_verbose_long(self):
        self.patch(sys, "argv", ["anode", "--config=" + FILE_CONFIG, "--verbose"])
        self.assert_anode(1, False, True, False, False, lambda: self.clock_tick(1))

    def test_main_quiet_short(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG, "-q"])
        self.assert_anode(1, False, True, False, False, lambda: self.clock_tick(1))

    def test_main_quiet_long(self):
        self.patch(sys, "argv", ["anode", "--config=" + FILE_CONFIG, "--quiet"])
        self.assert_anode(1, False, True, False, False, lambda: self.clock_tick(1))

    def test_main_verbose_short_null(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG, "-v"])
        self.assert_anode(1, False, False, True, False, lambda: self.clock_tick(1))

    def test_main_verbose_short_corrupt(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG, "-v"])
        self.assert_anode(1, False, False, False, True, lambda: self.clock_tick(1))

    def test_main_quiet_short_random(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_MIN, "-q"])
        self.assert_anode(-1, False, False, False, False, lambda: self.clock_tick(1))

    def test_main_verbose_short_oneoff(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG_MIN_PARTITIONS, "-q"])
        self.assert_anode(callback=lambda: self.clock_tick(101), url="/rest/?metrics=power&bins=1second&scope=history&format=json")[0]


class ANodeModelTest(TestCase):
    def __init__(self, *args, **kwargs):
        super(ANodeModelTest, self).__init__(*args, **kwargs)
        with gzip.open(FILE_DATUMS_CSV, 'rt') as file_csv:
            self.datums_dict = Plugin.datums_csv_to_dict(csv.DictReader(file_csv))
        self.datums_df = {"df": Plugin.datums_dict_to_df(self.datums_dict["dict"])}

    def test_size(self):
        datum_sizes_tmp = [["model", "format", "container", "memory", "disk"]]

        def datum_sizes_add(datums, datums_count, datums_model, datums_format, datums_container):
            size_disk = None
            if datums_model == "dict":
                datums = {datums_model: datums} if datums_container is not None else datums
                size_memory = sum(sys.getsizeof(datum) for datum in datums[datums_model]) + \
                              sum(sum(map(sys.getsizeof, datum.itervalues())) for datum in datums[datums_model]) + \
                              8 * len(datums[datums_model]) * len(DATUM_SCHEMA_JSON[7]["fields"]) + \
                              (0 if datums_container is None else sys.getsizeof(datums[datums_model]))
            else:
                datums = {datums_model: [datums]} if datums_container is not None else datums
                if datums_model == "df":
                    if datums_container is None:
                        size_memory = sum(sys.getsizeof(datum) for datum in datums[datums_model]) + \
                                      sum(sys.getsizeof(datum["data_df"]) for datum in datums[datums_model]) + \
                                      8 * len(datums[datums_model]) * (len(DATUM_SCHEMA_JSON[7]["fields"]) - 3)
                    else:
                        size_memory = sum(sys.getsizeof(datum) for datum in datums[datums_model]) + \
                                      sum(sys.getsizeof(datum_df) for datum in datums[datums_model] for datum_df in datum)
                else:
                    size_memory = sum(sys.getsizeof(datum) for datum in datums[datums_model])
                    with tempfile.NamedTemporaryFile() as datums_file:
                        for datum in datums[datums_model]:
                            datums_file.write(datum)
                        datums_file.flush()
                        size_disk = os.path.getsize(datums_file.name)
            datum_sizes_tmp.append([datums_model, datums_format, datums_container, size_memory / datums_count,
                                    size_disk / datums_count if size_disk is not None else None])

        datums_dict_count = len(self.datums_dict["dict"])
        for datum_format in [("dict", "long", None, self.datums_dict), ("df", "wide", None, self.datums_df), ("avro", "long", None, self.datums_dict),
                             ("csv", "long", None, self.datums_dict), ("json", "long", None, self.datums_dict)]:
            datum_sizes_add(Plugin.datum_to_format(datum_format[3], datum_format[0], {}), datums_dict_count, datum_format[0], datum_format[1],
                            datum_format[2])
        for datum_format in [("dict", "long", "list", self.datums_dict), ("df", "wide", "list", self.datums_df),
                             ("csv", "wide", "csv", self.datums_df), ("json", "long", "json", self.datums_dict)]:
            datum_sizes_add(Plugin.datums_to_format(datum_format[3], datum_format[0], {}), datums_dict_count, datum_format[0], datum_format[1],
                            datum_format[2])
        print("Datum sizes:\n" + tabulate(datum_sizes_tmp, tablefmt='grid'))


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
        self.content = ANodeTest.substitute(HTTP_GETS[response.url]) if response.code == 200 else HTTP_GETS["http_404"]

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
test_randomise = False
test_repeats = True
test_nulls = False
test_corruptions = False

TEMPLATE_INTEGER_MAX = 8
TIME_TICK_PERIOD = 60 * 60
TIME_BOOT = calendar.timegm(time.gmtime()) - 2
DIR_ROOT = os.path.dirname(__file__) + "/../../"

FILE_CONFIG = DIR_ROOT + "config/anode.yaml"
FILE_CONFIG_MIN = DIR_ROOT + "anode/test/data/anode_min.yaml"
FILE_CONFIG_MIN_TICKS = DIR_ROOT + "anode/test/data/anode_min_ticks.yaml"
FILE_CONFIG_MIN_PARTITIONS = DIR_ROOT + "anode/test/data/anode_min_partitions.yaml"

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
