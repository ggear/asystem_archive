from __future__ import print_function

import abc
import base64
import calendar
import io
import json
import logging
import numbers
import operator
import os
import time
from collections import deque
from decimal import Decimal
from functools import reduce
from importlib import import_module
from uuid import getnode as get_mac

import avro
import avro.io
import avro.schema
import avro.schema
from avro.io import AvroTypeException
from bs4.dammit import EntitySubstitution
from twisted.internet import threads

import anode.plugin


class Plugin(object):
    def poll(self):
        if self.has_poll:
            if logging.getLogger().isEnabledFor(logging.INFO):
                time_start = time.time()
            self._poll()
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.getLogger().info("Plugin [{}] poll on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))

    def push(self, text_content):
        if self.has_push:
            if logging.getLogger().isEnabledFor(logging.INFO):
                start_time = time.time()
            self._push(text_content)
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.getLogger().info("Plugin [{}] push on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))

    def datum_pop(self):
        datums_popped = 0
        matric_name = "anode." + self.name + "."
        time_now = calendar.timegm(time.gmtime())
        self.datum_push(
            matric_name + "uptime",
            "current", "integral",
            self.datum_value((time_now - self.time_boot) / Decimal(24 * 60 * 60), factor=100),
            "d",
            100,
            time_now,
            time_now,
            1,
            "alltime",
            data_bound_lower=0,
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True
        )
        self.datum_push(
            matric_name + "lastseen",
            "current", "point",
            self.datum_value(self.time_seen),
            "scalor",
            1,
            time_now,
            time_now,
            1,
            "alltime",
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True

        )
        self.datum_push(
            matric_name + "buffer",
            "current", "point",
            self.datum_value(0 if ("history_ticks" not in self.config or self.config["history_ticks"] < 1) else (
                float(self.datums_history) / self.config["history_ticks"] * 100)),
            "%",
            1,
            time_now,
            time_now,
            1,
            "alltime",
            data_bound_upper=100,
            data_bound_lower=0,
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True
        )
        self.datum_push(
            matric_name + "metrics",
            "current", "point",
            self.datum_value(sum(len(types) for metrics in self.datums.values() for types in metrics.values())),
            "scalor",
            1,
            time_now,
            time_now,
            1,
            "alltime",
            data_bound_lower=0,
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True
        )
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] dropped [{}] datums".format(self.name, self.datums_dropped))
            logging.getLogger().info("Plugin [{}] pushed [{}] datums".format(self.name, self.datums_pushed))
            logging.getLogger().info("Plugin [{}] saved [{}] datums".format(self.name, self.datums_history))
        if "push_upstream" in self.config and self.config["push_upstream"]:
            for datum_metric in self.datums:
                for datum_type in self.datums[datum_metric]:
                    for datum_bin in self.datums[datum_metric][datum_type]:
                        # noinspection PyCompatibility
                        for i in xrange(len(self.datums[datum_metric][datum_type][datum_bin][DATUM_QUEUE_PUBLISH])):
                            datum_avro = self.datums[datum_metric][datum_type][datum_bin][DATUM_QUEUE_PUBLISH].popleft()
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.getLogger().debug("Plugin [{}] popped datum [{}]".format(
                                    self.name, self.datum_tostring(self.datum_avro_to_dict(datum_avro))))
                            # TDOD: push to QMTT broker, returning datums to left of deque if push fails
                            datums_popped += 1
        self.datum_push(
            matric_name + "queue",
            "current", "point",
            self.datum_value(self.datums_pushed - datums_popped),
            "datums",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_lower=0,
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True
        )
        self.datums_pushed = 0
        self.datums_dropped = 0
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] popped [{}] datums".format(self.name, datums_popped))

    def datum_push(self, data_metric, data_temporal, data_type, data_value, data_unit, data_scale, data_timestamp, bin_timestamp, bin_width,
                   bin_unit, data_bound_upper=None, data_bound_lower=None, data_derived_max=False, data_derived_min=False,
                   data_derived_period=1, data_derived_unit="day", data_transient=False):
        if data_value is not None:
            datum_dict = {
                "anode_id": ID_BYTE,
                "data_source": self.name,
                "data_metric": data_metric,
                "data_temporal": data_temporal,
                "data_type": data_type,
                "data_value": data_value,
                "data_unit": data_unit,
                "data_scale": data_scale,
                "data_timestamp": data_timestamp,
                "bin_timestamp": bin_timestamp,
                "bin_width": bin_width,
                "bin_unit": bin_unit
            }
            if data_bound_upper is not None and data_value > Decimal(data_bound_upper * data_scale):
                datum_dict["data_value"] = data_bound_upper * data_scale
                if logging.getLogger().isEnabledFor(logging.WARNING):
                    logging.getLogger().debug("Plugin [{}] upperbounded datum [{}]".format(
                        self.name, self.datum_tostring(datum_dict)))
            if data_bound_lower is not None and data_value < Decimal(data_bound_lower * data_scale):
                datum_dict["data_value"] = data_bound_lower * data_scale
                if logging.getLogger().isEnabledFor(logging.WARNING):
                    logging.getLogger().debug("Plugin [{}] lowerbounded datum [{}]".format(
                        self.name, self.datum_tostring(datum_dict)))
            try:
                datum_avro = self.datum_dict_to_avro(datum_dict)
            except AvroTypeException as error:
                if logging.getLogger().isEnabledFor(logging.ERROR):
                    logging.getLogger().error("Error serialising Avro object [{}]".format(error))
                return
            if datum_dict["data_metric"] not in self.datums:
                self.datums[datum_dict["data_metric"]] = {}
            if datum_dict["data_type"] not in self.datums[datum_dict["data_metric"]]:
                self.datums[datum_dict["data_metric"]][datum_dict["data_type"]] = {}
            if str(datum_dict["bin_width"]) + datum_dict["bin_unit"] not in self.datums[datum_dict["data_metric"]][datum_dict["data_type"]]:
                self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][str(datum_dict["bin_width"]) + datum_dict["bin_unit"]] = {
                    DATUM_QUEUE_PUBLISH: deque(
                        maxlen=(
                            None if "publish_ticks" not in self.config or self.config["publish_ticks"] < 1 else self.config["publish_ticks"])),
                    DATUM_QUEUE_HISTORY: deque()
                }
            datums_deref = self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][str(datum_dict["bin_width"]) + datum_dict["bin_unit"]]
            if DATUM_QUEUE_LAST not in datums_deref or datums_deref[DATUM_QUEUE_LAST]["data_value"] != datum_dict["data_value"] or \
                            datums_deref[DATUM_QUEUE_LAST]["data_unit"] != \
                            datum_dict["data_unit"] or datums_deref[DATUM_QUEUE_LAST]["data_scale"] != datum_dict["data_scale"]:
                self.time_seen = calendar.timegm(time.gmtime())
                datums_deref[DATUM_QUEUE_LAST] = datum_dict
                bin_timestamp_modulus = (datum_dict["bin_timestamp"] + self.time_tmz_offset) - \
                                        (datum_dict["bin_timestamp"] + self.time_tmz_offset) % \
                                        Plugin.get_seconds(data_derived_period, data_derived_unit) - self.time_tmz_offset
                if not data_transient:
                    if data_derived_max:
                        if DATUM_QUEUE_MAX not in datums_deref or datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] < bin_timestamp_modulus or \
                                        datums_deref[DATUM_QUEUE_MAX]["data_value"] < datum_dict["data_value"]:
                            datums_deref[DATUM_QUEUE_MAX] = datum_dict.copy()
                            datums_deref[DATUM_QUEUE_MAX]["bin_type"] = "high"
                            datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] = bin_timestamp_modulus if \
                                (DATUM_QUEUE_MAX in datums_deref and datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] < bin_timestamp_modulus) else \
                                calendar.timegm(time.gmtime())
                            datums_deref[DATUM_QUEUE_MAX]["bin_width"] = data_derived_period
                            datums_deref[DATUM_QUEUE_MAX]["bin_unit"] = data_derived_unit
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.getLogger().debug(
                                    "Plugin [{}] seleted high [{}]".format(self.name, self.datum_tostring(datums_deref[DATUM_QUEUE_MAX])))
                            self.datum_push(data_metric, data_temporal, "high", datum_dict["data_value"], data_unit, data_scale,
                                            datum_dict["data_timestamp"], datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"],
                                            datum_dict["bin_width"] if datum_dict["data_type"] == "integral"
                                            else data_derived_period, datum_dict["bin_unit"] if datum_dict["data_type"] == "integral"
                                            else data_derived_unit, data_transient=data_transient)
                    if data_derived_min:
                        if DATUM_QUEUE_MIN not in datums_deref or datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"] < bin_timestamp_modulus or \
                                        datums_deref[DATUM_QUEUE_MIN]["data_value"] > datum_dict["data_value"]:
                            datums_deref[DATUM_QUEUE_MIN] = datum_dict.copy()
                            datums_deref[DATUM_QUEUE_MIN]["bin_type"] = "low"
                            datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"] = bin_timestamp_modulus if \
                                (DATUM_QUEUE_MAX in datums_deref and datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] < bin_timestamp_modulus) else \
                                calendar.timegm(time.gmtime())
                            datums_deref[DATUM_QUEUE_MIN]["bin_width"] = data_derived_period
                            datums_deref[DATUM_QUEUE_MIN]["bin_unit"] = data_derived_unit
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.getLogger().debug(
                                    "Plugin [{}] deleted low [{}]".format(self.name, self.datum_tostring(datums_deref[DATUM_QUEUE_MIN])))
                            self.datum_push(data_metric, data_temporal, "low", datum_dict["data_value"], data_unit, data_scale,
                                            datum_dict["data_timestamp"], datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"],
                                            datum_dict["bin_width"] if datum_dict["data_type"] == "integral"
                                            else data_derived_period, datum_dict["bin_unit"] if datum_dict["data_type"] == "integral"
                                            else data_derived_unit, data_transient=data_transient)
                    self.datums_pushed += 1
                    datums_deref[DATUM_QUEUE_PUBLISH].append(datum_avro)
                    if "history_ticks" in self.config and self.config["history_ticks"] > 0 and \
                                    "history_seconds" in self.config and self.config["history_seconds"] > 0:
                        datum_history_peek = None
                        if len(datums_deref[DATUM_QUEUE_HISTORY]) > 0:
                            datum_history_peek = datums_deref[DATUM_QUEUE_HISTORY].popleft()
                        if datum_history_peek is not None:
                            if self.config["history_ticks"] <= self.datums_history or \
                                    ((calendar.timegm(time.gmtime()) - self.datum_avro_to_dict(datum_history_peek)[
                                        "bin_timestamp"]) >= self.config["history_seconds"]):
                                if logging.getLogger().isEnabledFor(logging.DEBUG):
                                    logging.getLogger().debug("Plugin [{}] seleted datum [{}]".format(self.name, self.datum_tostring(
                                        self.datum_avro_to_dict(datum_history_peek))))
                            else:
                                datums_deref[DATUM_QUEUE_HISTORY].appendleft(datum_history_peek)
                                self.datums_history += 1
                        datums_deref[DATUM_QUEUE_HISTORY].append(datum_avro)
                        if logging.getLogger().isEnabledFor(logging.DEBUG):
                            logging.getLogger().debug("Plugin [{}] saved datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.getLogger().debug("Plugin [{}] pushed datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
                self.anode.publish_datums([datum_dict])
            else:
                self.datums_dropped += 1
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.getLogger().debug("Plugin [{}] dropped datum [{}]".format(self.name, self.datum_tostring(datum_dict)))

    def datum_tostring(self, datum_dict):
        return "{}.{}.{}.{}{}.{}={}{}".format(
            datum_dict["data_source"], datum_dict["data_metric"], datum_dict["data_type"], datum_dict["bin_width"], datum_dict["bin_timestamp"],
            datum_dict["bin_unit"].encode("utf-8"),
            int(self.datum_avro_to_dict(self.datum_dict_to_avro(datum_dict))["data_value"]) / Decimal(datum_dict["data_scale"]),
            datum_dict["data_unit"].encode("utf-8")
        )

    def datum_get(self, datum_scope, data_metric, data_type, bin_width, bin_unit, data_derived_period=None, data_derived_unit="day"):
        if data_metric in self.datums and data_type in self.datums[data_metric] and \
                        (str(bin_width) + bin_unit) in self.datums[data_metric][data_type] and \
                        datum_scope in self.datums[data_metric][data_type][str(bin_width) + bin_unit]:
            datum_dict = self.datums[data_metric][data_type][str(bin_width) + bin_unit][datum_scope]
            if data_derived_period is None or (datum_dict["bin_width"] == data_derived_period and
                                                       datum_dict["bin_unit"] == data_derived_unit and
                                                           datum_dict["bin_timestamp"] %
                                                           Plugin.get_seconds(data_derived_period, data_derived_unit) == 0):
                return datum_dict
            else:
                return None

    def datums_filter_get(self, datum_filter, datum_format="dict"):
        if logging.getLogger().isEnabledFor(logging.INFO):
            time_start = time.time()
        datums_filtered = []
        for data_metric in self.datums:
            if Plugin.is_fitlered(datum_filter, "metrics", data_metric):
                for datum_type in self.datums[data_metric]:
                    if Plugin.is_fitlered(datum_filter, "types", datum_type):
                        for datum_bin in self.datums[data_metric][datum_type]:
                            if Plugin.is_fitlered(datum_filter, "bins", datum_bin):
                                datum_scopes = [DATUM_QUEUE_LAST] if "scope" not in datum_filter else datum_filter["scope"]
                                for datum_scope in datum_scopes:
                                    if datum_scope in self.datums[data_metric][datum_type][datum_bin]:
                                        if datum_scope == DATUM_QUEUE_LAST:
                                            if not Plugin.is_fitlered_len(datum_filter, datums_filtered):
                                                datums_filtered.append(Plugin.datum_dict_to_format(
                                                    self.datums[data_metric][datum_type][datum_bin][datum_scope], datum_format))
                                        else:
                                            for datum in self.datums[data_metric][datum_type][datum_bin][datum_scope]:
                                                if not Plugin.is_fitlered_len(datum_filter, datums_filtered):
                                                    datums_filtered.append(Plugin.datum_avro_to_format(datum, datum_format))
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] datums_filter_get on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))
        return datums_filtered

    @staticmethod
    def datums_filter(datum_filter, datums, datum_format="dict"):
        if logging.getLogger().isEnabledFor(logging.INFO):
            time_start = time.time()
        datums_filtered = []
        for datum in datums:
            if Plugin.is_fitlered(datum_filter, "metrics", datum["data_metric"]):
                if Plugin.is_fitlered(datum_filter, "types", datum["data_type"]):
                    if Plugin.is_fitlered(datum_filter, "bins", str(datum["bin_width"]) + datum["bin_unit"]):
                        if Plugin.is_fitlered_len(datum_filter, datums_filtered):
                            return datums_filtered
                        datums_filtered.append(Plugin.datum_dict_to_format(datum, datum_format))
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] datums_filter on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))
        return datums_filtered

    @staticmethod
    def is_fitlered_len(datum_filters, datums_filtered):
        return "limit" in datum_filters and min(datum_filters["limit"]).isdigit() and int(min(datum_filters["limit"])) <= len(datums_filtered)

    @staticmethod
    def datums_sort(datums):
        return sorted(datums, key=lambda datum: (datum["data_metric"],
                                                 "aaaaa" if datum["data_type"] == "point" else
                                                 "bbbbb" if datum["data_type"] == "mean" else
                                                 "ccccc" if datum["data_type"] == "integral" else
                                                 "ddddd" if datum["data_type"] == "low" else
                                                 "eeeee" if datum["data_type"] == "high" else
                                                 datum["data_type"],
                                                 "aaaaa" if datum["bin_unit"] == "second" else
                                                 "bbbbb" if datum["bin_unit"] == "minute" else
                                                 "ccccc" if datum["bin_unit"] == "hour" else
                                                 "ddddd" if datum["bin_unit"] == "lighthours" else
                                                 "eeeee" if datum["bin_unit"] == "darkhours" else
                                                 "fffff" if datum["bin_unit"] == "day" else
                                                 "ggggg" if datum["bin_unit"] == "month" else
                                                 "hhhhh" if datum["bin_unit"] == "year" else
                                                 "iiiii",
                                                 datum["bin_width"]))

    @staticmethod
    def is_fitlered(datum_filters, datum_parameter, datum_attribute):
        if datum_parameter not in datum_filters:
            return True
        for datum_filter in datum_filters[datum_parameter]:
            if datum_attribute.lower().find(datum_filter.lower()) != -1:
                return True
        return False

    @staticmethod
    def datum_avro_to_format(datum_avro, datum_format):
        datum = datum_avro
        if datum_format == "dict":
            datum = Plugin.datum_avro_to_dict(datum_avro)
        elif datum_format == "json":
            datum = Plugin.datum_dict_to_json(Plugin.datum_avro_to_dict(datum_avro))
        elif datum_format != "avro":
            raise ValueError("Unkown datum format [{}]".format(datum_format))
        return datum

    @staticmethod
    def datum_dict_to_format(datum_dict, datum_format):
        datum = datum_dict
        if datum_format == "json":
            datum = Plugin.datum_dict_to_json(datum_dict)
        elif datum_format == "avro":
            datum = Plugin.datum_dict_to_avro(datum_dict)
        elif datum_format != "dict":
            raise ValueError("Unkown datum format [{}]".format(datum_format))
        return datum

    @staticmethod
    def datum_dict_to_avro(datum_dict):
        avro_writer = io.BytesIO()
        avro.io.DatumWriter(DATUM_SCHEMA_AVRO).write(datum_dict, avro.io.BinaryEncoder(avro_writer))
        return avro_writer.getvalue()

    @staticmethod
    def datum_avro_to_dict(datum_avro):
        return avro.io.DatumReader(DATUM_SCHEMA_AVRO).read(avro.io.BinaryDecoder(io.BytesIO(datum_avro)))

    @staticmethod
    def datum_dict_to_json(datum_dict):
        datum_dict = datum_dict.copy()
        if "anode_id" in datum_dict:
            datum_dict["anode_id"] = ID_HEX
        return json.dumps(datum_dict, separators=(',', ':'))

    @staticmethod
    def datums_dict_to_json(datums_dict):
        if logging.getLogger().isEnabledFor(logging.INFO):
            time_start = time.time()
        for datum_dict in datums_dict:
            datum_dict["anode_id"] = ID_HEX
        datums_json = json.dumps(datums_dict, separators=(',', ':'))
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [*] datums_dict_to_json off-thread [{}] ms".format(str(int((time.time() - time_start) * 1000))))
        return datums_json

    @staticmethod
    def datums_dict_to_csv(datums_dict):
        if logging.getLogger().isEnabledFor(logging.INFO):
            time_start = time.time()
        datums_unit = {}
        for datum_dict in datums_dict:
            datum_dict["anode_id"] = ID_BASE64
            if datum_dict["data_unit"] not in datums_unit:
                datums_unit[datum_dict["data_unit"]] = EntitySubstitution().substitute_html(datum_dict["data_unit"])
            datum_dict["data_unit"] = datums_unit[datum_dict["data_unit"]]
            if datum_dict["bin_unit"] not in datums_unit:
                datums_unit[datum_dict["bin_unit"]] = EntitySubstitution().substitute_html(datum_dict["bin_unit"])
            datum_dict["bin_unit"] = datums_unit[datum_dict["bin_unit"]]
        datums_csv = (','.join(str(datum_dict_key) for datum_dict_key in datums_dict[0].iterkeys()) + "\n" if len(datums_dict) > 0 else "") + \
                     ("\n".join([",".join(str(datum_dict_value) for datum_dict_value in datum_dict.itervalues()) for datum_dict in datums_dict])) \
                     + "\n"
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [*] datums_dict_to_csv off-thread [{}] ms".format(str(int((time.time() - time_start) * 1000))))
        return datums_csv

    @staticmethod
    def datums_dict_to_format(datums_dict, datum_format):
        if logging.getLogger().isEnabledFor(logging.INFO):
            time_start = time.time()
        if datum_format == "dict":
            return datums_dict
        datums_copy = []
        for datum_dict in datums_dict:
            datums_copy.append(datum_dict.copy())
        if datum_format == "json":
            datums_dict_to_format = Plugin.datums_dict_to_json
        elif datum_format == "csv":
            datums_dict_to_format = Plugin.datums_dict_to_csv
        else:
            raise ValueError("Unkown datum format [{}]".format(datum_format))
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info(
                "Plugin [*] datums_dict_to_format on-thread [{}] ms".format(str(int((time.time() - time_start) * 1000))))
        return threads.deferToThread(datums_dict_to_format, datums_copy)

    def datum_value(self, data, keys=None, default=None, factor=1):
        # noinspection PyBroadException
        try:
            value = data if keys is None else reduce(operator.getitem, keys, data)
            if value is None:
                value = default
                if logging.getLogger().isEnabledFor(logging.WARNING):
                    logging.getLogger().warning(
                        "Plugin [{}] setting value {} to default [{}] from response [{}]".format(self.name, keys, default, data))
            return value if not isinstance(value, numbers.Number) else int(value * factor)
        except ValueError:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.exception("Unexpected error processing value {} from response [{}]".format(keys, data))
            return None if default is None else int(default * factor)

    @staticmethod
    def get_seconds(scalor, unit):
        if unit == "second":
            return scalor
        elif unit == "minute":
            return scalor * 60
        elif unit == "hour":
            return scalor * 60 * 60
        elif unit == "day":
            return scalor * 60 * 60 * 24
        elif unit == "month":
            return scalor * 60 * 60 * 24 * 30.42
        elif unit == "year":
            return scalor * 60 * 60 * 24 * 365
        else:
            raise Exception("Unknown time unit [{}]".format(unit))

    @staticmethod
    def get(parent, module, config):
        plugin = getattr(import_module("anode.plugin") if hasattr(anode.plugin, module.title()) else
                         import_module("anode.plugin." + module), module.title())(parent, module, config)
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] initialised".format(module))
        return plugin

    __metaclass__ = abc.ABCMeta

    def __init__(self, parent, name, config):
        self.has_poll = getattr(self, "_poll", None) is not None
        self.has_push = getattr(self, "_push", None) is not None
        self.anode = parent
        self.name = name
        self.config = config
        self.datums = {}
        self.datums_pushed = 0
        self.datums_dropped = 0
        self.datums_history = 0
        self.time_seen = None
        self.time_boot = calendar.timegm(time.gmtime())
        time_local = time.localtime()
        self.time_tmz_offset = calendar.timegm(time_local) - calendar.timegm(time.gmtime(time.mktime(time_local)))


DATUM_QUEUE_MIN = "min"
DATUM_QUEUE_MAX = "max"
DATUM_QUEUE_LAST = "last"
DATUM_QUEUE_PUBLISH = "publish"
DATUM_QUEUE_HISTORY = "history"

DATUM_SCHEMA_FILE = open(os.path.dirname(__file__) + "/../model/datum.avsc", "rb").read()
DATUM_SCHEMA_JSON = json.loads(DATUM_SCHEMA_FILE)
DATUM_SCHEMA_AVRO = avro.schema.parse(DATUM_SCHEMA_FILE)

ID_BYTE = format(get_mac(), "x").decode("hex")
ID_HEX = ID_BYTE.encode("hex")
ID_BASE64 = base64.b64encode(ID_BYTE)


class Callback(Plugin):
    def _poll(self):
        if "callback" in self.config and self.config["callback"] is not None:
            self.config["callback"]()
