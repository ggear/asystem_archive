from __future__ import print_function

import abc
import calendar
import io
import json
import logging
import numbers
import operator
import os
import time
from calendar import timegm
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

import anode.plugin


class Plugin(object):
    # noinspection PyStatementEffect
    def poll(self):
        None

    # noinspection PyStatementEffect
    def push(self, text_content):
        None

    def datum_pop(self):
        datums_popped = 0
        matric_name = "anode." + self.name + "."
        timestamp = calendar.timegm(time.gmtime())
        self.datum_push(
            matric_name + "buffer",
            "current", "point",
            self.datum_value(0 if ("history_ticks" not in self.config or self.config["history_ticks"] < 1) else (
                float(self.datums_history) / self.config["history_ticks"] * 100)),
            "%",
            1,
            timestamp,
            timestamp,
            1,
            "alltime",
            data_bound_upper=100,
            data_bound_lower=0,
            data_transient=True
        )
        self.datum_push(
            matric_name + "metrics",
            "current", "point",
            self.datum_value(sum(len(types) for metrics in self.datums.values() for types in metrics.values())),
            "scalor",
            1,
            timestamp,
            timestamp,
            1,
            "alltime",
            data_bound_lower=0,
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
                                logging.getLogger().debug("Popped datum [{}]".format(self.datum_tostring(self.datum_avro_to_dict(datum_avro))))
                            # TDOD: push to QMTT broker, returning datums to left of deque if push fails
                            datums_popped += 1
        self.datums_pushed = 0
        self.datums_dropped = 0
        self.datum_push(
            matric_name + "queue",
            "current", "point",
            self.datum_value(self.datums_pushed - datums_popped),
            "datums",
            1,
            timestamp,
            timestamp,
            self.config["poll_seconds"],
            "second",
            data_bound_lower=0,
            data_transient=True
        )
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] popped [{}] datums".format(self.name, datums_popped))

    def datum_push(self, data_metric, data_temporal, data_type, data_value, data_unit, data_scale, data_timestamp, bin_timestamp, bin_width,
                   bin_unit, data_bound_upper=None, data_bound_lower=None, data_derived_max=False, data_derived_min=False,
                   data_derived_period_sec=86400, data_derived_period=1, data_derived_unit="day", data_transient=False):
        if data_value is not None:
            datum_dict = {
                "anode_id": self.mac_address,
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
                    logging.getLogger().debug("Upperbounded datum [{}]".format(
                        self.datum_tostring(datum_dict)))
            if data_bound_lower is not None and data_value < Decimal(data_bound_lower * data_scale):
                datum_dict["data_value"] = data_bound_lower * data_scale
                if logging.getLogger().isEnabledFor(logging.WARNING):
                    logging.getLogger().debug("Lowerbounded datum [{}]".format(
                        self.datum_tostring(datum_dict)))
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
                datums_deref[DATUM_QUEUE_LAST] = datum_dict
                if not data_transient:
                    datums_deref[DATUM_QUEUE_PUBLISH].append(datum_avro)
                    self.datums_pushed += 1
                    bin_timestamp_day = (datum_dict["bin_timestamp"] + self.time_tmz_offset) - \
                                        (datum_dict["bin_timestamp"] + self.time_tmz_offset) % \
                                        data_derived_period_sec - self.time_tmz_offset
                    if data_derived_max:
                        if DATUM_QUEUE_MAX not in datums_deref or datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] < bin_timestamp_day or \
                                        datums_deref[DATUM_QUEUE_MAX]["data_value"] < datum_dict["data_value"]:
                            datums_deref[DATUM_QUEUE_MAX] = datum_dict.copy()
                            datums_deref[DATUM_QUEUE_MAX]["bin_type"] = "high"
                            datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] = bin_timestamp_day
                            datums_deref[DATUM_QUEUE_MAX]["bin_width"] = data_derived_period
                            datums_deref[DATUM_QUEUE_MAX]["bin_unit"] = data_derived_unit
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.getLogger().debug(
                                    "Deleted high [{}]".format(self.datum_tostring(datums_deref[DATUM_QUEUE_MAX])))
                            self.datum_push(data_metric, data_temporal, "high", datum_dict["data_value"], data_unit, data_scale, bin_timestamp_day,
                                            calendar.timegm(time.gmtime()), data_derived_period, data_derived_unit)
                    if data_derived_min:
                        if DATUM_QUEUE_MIN not in datums_deref or datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"] < bin_timestamp_day or \
                                        datums_deref[DATUM_QUEUE_MIN]["data_value"] > datum_dict["data_value"]:
                            datums_deref[DATUM_QUEUE_MIN] = datum_dict.copy()
                            datums_deref[DATUM_QUEUE_MIN]["bin_type"] = "low"
                            datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"] = bin_timestamp_day
                            datums_deref[DATUM_QUEUE_MIN]["bin_width"] = data_derived_period
                            datums_deref[DATUM_QUEUE_MIN]["bin_unit"] = data_derived_unit
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.getLogger().debug(
                                    "Deleted low [{}]".format(self.datum_tostring(datums_deref[DATUM_QUEUE_MIN])))
                            self.datum_push(data_metric, data_temporal, "low", datum_dict["data_value"], data_unit, data_scale, bin_timestamp_day,
                                            calendar.timegm(time.gmtime()), data_derived_period, data_derived_unit)
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
                                    logging.getLogger().debug(
                                        "Deleted datum [{}]".format(self.datum_tostring(self.datum_avro_to_dict(datum_history_peek))))
                            else:
                                datums_deref[DATUM_QUEUE_HISTORY].appendleft(datum_history_peek)
                                self.datums_history += 1
                        datums_deref[DATUM_QUEUE_HISTORY].append(datum_avro)
                        if logging.getLogger().isEnabledFor(logging.DEBUG):
                            logging.getLogger().debug("Saved datum [{}]".format(self.datum_tostring(datum_dict)))
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.getLogger().debug("Pushed datum [{}]".format(self.datum_tostring(datum_dict)))
                self.anode.publish_datums([datum_dict])
            else:
                self.datums_dropped += 1
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.getLogger().debug(
                        "Dropped datum [{}]".format(self.datum_tostring(datum_dict)))

    def datum_tostring(self, datum_dict):
        return "{}.{}.{}.{}{}.{}={}{}".format(
            datum_dict["data_source"], datum_dict["data_metric"], datum_dict["data_type"], datum_dict["bin_width"], datum_dict["bin_timestamp"],
            datum_dict["bin_unit"].encode('utf-8'),
            int(self.datum_avro_to_dict(self.datum_dict_to_avro(datum_dict))["data_value"]) / Decimal(datum_dict["data_scale"]),
            datum_dict["data_unit"].encode('utf-8')
        )

    def datums_filter_get(self, datum_filter, datum_format="dict"):
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
                                            for datum in reversed(self.datums[data_metric][datum_type][datum_bin][datum_scope]):
                                                if not Plugin.is_fitlered_len(datum_filter, datums_filtered):
                                                    datums_filtered.append(Plugin.datum_avro_to_format(datum, datum_format))
        return datums_filtered

    @staticmethod
    def datums_filter(datum_filter, datums, datum_format="dict"):
        datums_filtered = []
        for datum in datums:
            if Plugin.is_fitlered(datum_filter, "metrics", datum["data_metric"]):
                if Plugin.is_fitlered(datum_filter, "types", datum["data_type"]):
                    if Plugin.is_fitlered(datum_filter, "bins", str(datum["bin_width"]) + datum["bin_unit"]):
                        if Plugin.is_fitlered(datum_filter, "types", datum["data_type"]):
                            if Plugin.is_fitlered_len(datum_filter, datums_filtered):
                                return datums_filtered
                            datums_filtered.append(Plugin.datum_dict_to_format(datum, datum_format))
        return datums_filtered

    @staticmethod
    def is_fitlered_len(datum_filters, datums_filtered):
        return "limit" in datum_filters and min(datum_filters["limit"]).isdigit() and int(min(datum_filters["limit"])) <= len(datums_filtered)

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
        avro.io.DatumWriter(DATUM_SCHEMA).write(datum_dict, avro.io.BinaryEncoder(avro_writer))
        return avro_writer.getvalue()

    @staticmethod
    def datum_avro_to_dict(datum_avro):
        return avro.io.DatumReader(DATUM_SCHEMA).read(avro.io.BinaryDecoder(io.BytesIO(datum_avro)))

    @staticmethod
    def datum_dict_to_json(datum_dict):
        datum_dict = datum_dict.copy()
        if "anode_id" in datum_dict:
            datum_dict["anode_id"] = datum_dict["anode_id"].encode("hex")
        return json.dumps(datum_dict, separators=(',', ':'))

    @staticmethod
    def datums_dict_to_json(datums_dict):
        datums_json = []
        for datum_dict in datums_dict:
            datum_json = datum_dict.copy()
            if "anode_id" in datum_json:
                datum_json["anode_id"] = datum_json["anode_id"].encode("hex")
            datums_json.append(datum_json)
        return json.dumps(datums_json, separators=(',', ':'))

    @staticmethod
    def datum_value(data, keys=None, default=None, factor=1):
        # noinspection PyBroadException
        try:
            value = data if keys is None else reduce(operator.getitem, keys, data)
            if value is None:
                value = default
                if logging.getLogger().isEnabledFor(logging.WARNING):
                    logging.getLogger().warning("Setting value {} to default [{}] from response [{}]".format(keys, default, data))
            return value if not isinstance(value, numbers.Number) else int(value * factor)
        except Exception:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.exception("Unexpected error processing value {} from response [{}]".format(keys, data))
            return None if default is None else int(default * factor)

    @staticmethod
    def get(parent, module, config):
        plugin = getattr(import_module("anode.plugin") if hasattr(anode.plugin, module.title()) else
                         import_module("anode.plugin." + module), module.title())(parent, module, config)
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] initialised".format(module))
        return plugin

    __metaclass__ = abc.ABCMeta

    def __init__(self, parent, name, config):
        self.anode = parent
        self.name = name
        self.config = config
        self.mac_address = format(get_mac(), "x").decode("hex")
        self.datums = {}
        self.datums_pushed = 0
        self.datums_dropped = 0
        self.datums_history = 0
        current_time = time.localtime()
        self.time_tmz_offset = timegm(current_time) - timegm(time.gmtime(time.mktime(current_time)))


DATUM_QUEUE_MIN = "min"
DATUM_QUEUE_MAX = "max"
DATUM_QUEUE_LAST = "last"
DATUM_QUEUE_PUBLISH = "publish"
DATUM_QUEUE_HISTORY = "history"

DATUM_SCHEMA = avro.schema.parse(open(os.path.dirname(__file__) + "/../model/datum.avsc", "rb").read())


class Callback(Plugin):
    def poll(self):
        if "callback" in self.config and self.config["callback"] is not None:
            self.config["callback"]()
