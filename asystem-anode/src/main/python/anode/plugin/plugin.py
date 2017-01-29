from __future__ import print_function

import HTMLParser
import abc
import base64
import calendar
import decimal
import io
import json
import logging
import numbers
import operator
import os
import time
import urllib
from collections import deque
from decimal import Decimal
from functools import reduce
from importlib import import_module
from uuid import getnode as get_mac

import avro
import avro.io
import avro.schema
import avro.schema
import numpy
import pandas
from avro.io import AvroTypeException
from twisted.internet.task import Clock

import anode.plugin


class Plugin(object):
    def poll(self):
        if self.has_poll:
            log_timer = anode.Log(logging.DEBUG).start()
            self._poll()
            log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.poll)

    def push(self, text_content):
        if self.has_push:
            log_timer = anode.Log(logging.DEBUG).start()
            self._push(text_content)
            log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.push)

    def datum_pop(self):
        datums_popped = 0
        matric_name = "anode." + self.name + "."
        time_now = self.get_time()
        metrics_count = sum(len(units) for metrics in self.datums.values() for types in metrics.values() for units in types.values())
        self.datum_push(
            matric_name + "metrics",
            "current", "point",
            self.datum_value(metrics_count),
            "scalor",
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
        self.datum_push(
            matric_name + "buffer",
            "current", "point",
            self.datum_value(0 if metrics_count == 0 else (float(self.datums_buffer) / (self.datums_buffer_batch * metrics_count) * 100)),
            "%",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_upper=100,
            data_bound_lower=0,
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True
        )
        self.datum_push(
            matric_name + "history",
            "current", "point",
            self.datum_value(0 if ("history_ticks" not in self.config or self.config["history_ticks"] < 1) else (
                float(self.datums_history) / self.config["history_ticks"] * 100)),
            "%",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_upper=100,
            data_bound_lower=0,
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True
        )
        self.datum_push(
            matric_name + "days",
            "current", "point",
            self.datum_value(0 if ("history_days" not in self.config or self.config["history_days"] < 1) else (
                float(self.datums_days) / self.config["history_days"] * 100)),
            "%",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_upper=100,
            data_bound_lower=0,
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True
        )
        self.datum_push(
            matric_name + "uptime",
            "current", "integral",
            self.datum_value((time_now - self.time_boot) / Decimal(24 * 60 * 60), factor=100),
            "d",
            100,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
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
            self.config["poll_seconds"],
            "second",
            data_derived_max=True,
            data_derived_min=True,
            data_transient=True

        )
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] dropped [{}] datums".format(self.name, self.datums_dropped))
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] pushed datums".format(self.name, self.datums_pushed))
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] saved datums".format(self.name, self.datums_history))
        if "push_upstream" in self.config and self.config["push_upstream"]:
            for datum_metric in self.datums:
                for datum_type in self.datums[datum_metric]:
                    for datum_unit in self.datums[datum_metric][datum_type]:
                        for datum_bin in self.datums[datum_metric][datum_type][datum_unit]:
                            # noinspection PyCompatibility
                            for i in xrange(len(self.datums[datum_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_PUBLISH])):
                                datum_avro = self.datums[datum_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_PUBLISH].popleft()
                                anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] popped datum [{}]".format(
                                    self.name, self.datum_tostring(self.datum_avro_to_dict(datum_avro)[0])))
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
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] popped [{}] datums".format(self.name, datums_popped))

    def datum_push(self, data_metric, data_temporal, data_type, data_value, data_unit, data_scale, data_timestamp, bin_timestamp, bin_width,
                   bin_unit, data_bound_upper=None, data_bound_lower=None, data_derived_max=False, data_derived_min=False,
                   data_derived_period=1, data_derived_unit="day", data_transient=False):
        log_timer = anode.Log(logging.DEBUG).start()
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
                anode.Log(logging.DEBUG).log("Plugin", "state",
                                             lambda: "[{}] upperbounded datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
            if data_bound_lower is not None and data_value < Decimal(data_bound_lower * data_scale):
                datum_dict["data_value"] = data_bound_lower * data_scale
                anode.Log(logging.DEBUG).log("Plugin", "state",
                                             lambda: "[{}] lowerbounded datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
            try:
                datum_avro = self.datum_dict_to_avro(datum_dict)[0]
            except AvroTypeException as exception:
                anode.Log(logging.ERROR).log("Plugin", "error",
                                             lambda: "[{}] error serialising Avro object [{}]".format(self.name, exception), exception)
                return
            if datum_dict["data_metric"] not in self.datums:
                self.datums[datum_dict["data_metric"]] = {}
            if datum_dict["data_type"] not in self.datums[datum_dict["data_metric"]]:
                self.datums[datum_dict["data_metric"]][datum_dict["data_type"]] = {}
            if datum_dict["data_unit"] not in self.datums[datum_dict["data_metric"]][datum_dict["data_type"]]:
                self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][datum_dict["data_unit"]] = {}
            if str(datum_dict["bin_width"]) + datum_dict["bin_unit"] not in \
                    self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][datum_dict["data_unit"]]:
                self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][datum_dict["data_unit"]][
                    str(datum_dict["bin_width"]) + datum_dict["bin_unit"]] = {
                    DATUM_QUEUE_PUBLISH: deque(
                        maxlen=(None if "publish_ticks" not in self.config or self.config["publish_ticks"] < 1 else self.config["publish_ticks"])),
                    DATUM_QUEUE_BUFFER: deque(),
                    DATUM_QUEUE_HISTORY: {}
                }
            datums_deref = self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][datum_dict["data_unit"]][str(datum_dict["bin_width"]) +
                                                                                                                    datum_dict["bin_unit"]]
            if DATUM_QUEUE_LAST not in datums_deref or datums_deref[DATUM_QUEUE_LAST]["data_value"] != datum_dict["data_value"] or \
                            datums_deref[DATUM_QUEUE_LAST]["data_unit"] != \
                            datum_dict["data_unit"] or datums_deref[DATUM_QUEUE_LAST]["data_scale"] != datum_dict["data_scale"]:
                self.time_seen = self.get_time()
                datums_deref[DATUM_QUEUE_LAST] = datum_dict
                bin_timestamp_derived = self.get_time_period(datum_dict["bin_timestamp"],
                                                             Plugin.get_seconds(data_derived_period, data_derived_unit))
                if not data_transient:
                    if data_derived_max:
                        if DATUM_QUEUE_MAX not in datums_deref or datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] < bin_timestamp_derived or \
                                        datums_deref[DATUM_QUEUE_MAX]["data_value"] < datum_dict["data_value"]:
                            datums_deref[DATUM_QUEUE_MAX] = datum_dict.copy()
                            datums_deref[DATUM_QUEUE_MAX]["bin_type"] = "high"
                            datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] = bin_timestamp_derived if \
                                (DATUM_QUEUE_MAX in datums_deref and datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] < bin_timestamp_derived) else \
                                self.get_time()
                            datums_deref[DATUM_QUEUE_MAX]["bin_width"] = data_derived_period
                            datums_deref[DATUM_QUEUE_MAX]["bin_unit"] = data_derived_unit
                            anode.Log(logging.DEBUG).log("Plugin", "state",
                                                         lambda: "[{}] seleted high [{}]".format(self.name,
                                                                                                 self.datum_tostring(datums_deref[DATUM_QUEUE_MAX])))
                            self.datum_push(data_metric, data_temporal, "high", datum_dict["data_value"], data_unit, data_scale,
                                            datum_dict["data_timestamp"], datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"],
                                            datum_dict["bin_width"] if datum_dict["data_type"] == "integral"
                                            else data_derived_period, datum_dict["bin_unit"] if datum_dict["data_type"] == "integral"
                                            else data_derived_unit, data_transient=data_transient)
                    if data_derived_min:
                        if DATUM_QUEUE_MIN not in datums_deref or datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"] < bin_timestamp_derived or \
                                        datums_deref[DATUM_QUEUE_MIN]["data_value"] > datum_dict["data_value"]:
                            datums_deref[DATUM_QUEUE_MIN] = datum_dict.copy()
                            datums_deref[DATUM_QUEUE_MIN]["bin_type"] = "low"
                            datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"] = bin_timestamp_derived if \
                                (DATUM_QUEUE_MAX in datums_deref and datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] < bin_timestamp_derived) else \
                                self.get_time()
                            datums_deref[DATUM_QUEUE_MIN]["bin_width"] = data_derived_period
                            datums_deref[DATUM_QUEUE_MIN]["bin_unit"] = data_derived_unit
                            anode.Log(logging.DEBUG).log("Plugin", "state",
                                                         lambda: "[{}] deleted low [{}]".format(self.name,
                                                                                                self.datum_tostring(datums_deref[DATUM_QUEUE_MIN])))
                            self.datum_push(data_metric, data_temporal, "low", datum_dict["data_value"], data_unit, data_scale,
                                            datum_dict["data_timestamp"], datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"],
                                            datum_dict["bin_width"] if datum_dict["data_type"] == "integral"
                                            else data_derived_period, datum_dict["bin_unit"] if datum_dict["data_type"] == "integral"
                                            else data_derived_unit, data_transient=data_transient)
                    self.datums_pushed += 1
                    datums_deref[DATUM_QUEUE_PUBLISH].append(datum_avro)
                    if "history_ticks" in self.config and self.config["history_ticks"] > 0 and \
                                    "history_days" in self.config and self.config["history_days"] > 0:
                        bin_timestamp_day = self.get_time_period(datum_dict["bin_timestamp"], Plugin.get_seconds(1, "day"))
                        if len(datums_deref[DATUM_QUEUE_BUFFER]) == self.datums_buffer_batch or bin_timestamp_day \
                                not in datums_deref[DATUM_QUEUE_HISTORY]:
                            self.datum_merge_buffer_history(datums_deref[DATUM_QUEUE_BUFFER], datums_deref[DATUM_QUEUE_HISTORY])
                        datums_deref[DATUM_QUEUE_BUFFER].append(datum_dict)
                        self.datums_buffer += 1
                        self.datums_history += 1
                        anode.Log(logging.DEBUG).log("Plugin", "state",
                                                     lambda: "[{}] saved datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
                anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] pushed datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
                self.anode.push_datums({"dict": [datum_dict]})
            else:
                self.datums_dropped += 1
                anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] dropped datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.datum_push)

    def datum_merge_buffer_history(self, datums_buffer, datums_history):
        log_timer = anode.Log(logging.DEBUG).start()
        if "history_ticks" in self.config and self.config["history_ticks"] > 0 and \
                        "history_days" in self.config and self.config["history_days"] > 0:
            if len(datums_buffer) > 0:
                bin_timestamp_day = self.get_time_period(datums_buffer[0]["bin_timestamp"], Plugin.get_seconds(1, "day"))
                datums_df = self.datums_dict_to_df(datums_buffer)
                if len(datums_df) != 1:
                    raise ValueError("Assertion error merging mixed datum types when there should not be any!")
                datums_df = datums_df[0]
                datums_buffer.clear()
                self.datums_buffer = 0
                if bin_timestamp_day not in datums_history:
                    datums_history[bin_timestamp_day] = datums_df
                else:
                    datums_history[bin_timestamp_day]["data_df"] = pandas.concat([datums_history[bin_timestamp_day]["data_df"], datums_df["data_df"]],
                                                                                 ignore_index=True)
                    anode.Log(logging.DEBUG).log("Plugin", "state",
                                                 lambda: "[{}] merged buffer partition [{}]".format(self.name, bin_timestamp_day))
                bin_timestamp_day_expireds = []
                bin_timestamp_day_upper = bin_timestamp_day + self.config["history_days"] * Plugin.get_seconds(1, "day")
                for bin_timestamp_day_cached in datums_history:
                    if bin_timestamp_day_cached > bin_timestamp_day_upper:
                        bin_timestamp_day_expireds.append(bin_timestamp_day_cached)
                for bin_timestamp_day_expired in bin_timestamp_day_expireds:
                    del datums_history[bin_timestamp_day_expired]
                    anode.Log(logging.DEBUG).log("Plugin", "state",
                                                 lambda: "[{}] purged expired partition [{}]".format(self.name, bin_timestamp_day_expired))
                while len(datums_history) > self.config["history_days"] or \
                                sum(len(datums_df_cached["data_df"].index) for datums_df_cached in datums_history.itervalues()) > self.config[
                            "history_ticks"]:
                    bin_timestamp_day_upperbounded = max(datums_history.iterkeys())
                    del datums_history[bin_timestamp_day_upperbounded]
                    anode.Log(logging.DEBUG).log("Plugin", "state",
                                                 lambda: "[{}] purged upperbounded partition [{}]".format(self.name, bin_timestamp_day_upperbounded))
                self.datums_days = len(datums_history)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.datum_merge_buffer_history)

    def datum_merge_history(self, datums_history, datum_filter):
        datums = []
        datums_day_metadata = {}
        datums_day_upper = self.get_time_period(self.get_time(), Plugin.get_seconds(1, "day")) + \
                           (0 if "days" not in datum_filter else max(datum_filter["days"])) * Plugin.get_seconds(1, "day")
        datums_day_partitions = []
        for datums_day in datums_history:
            if datums_day <= datums_day_upper:
                datums_day_metadata = datums_history[datums_day]
                datums_day_partitions.append(datums_day_metadata["data_df"])
        if len(datums_day_partitions) > 0:
            if len(datums_day_partitions) == 1:
                datums_day_metadata["data_df"] = datums_day_partitions[0].copy(deep=False)
            else:
                datums_day_metadata["data_df"] = pandas.concat(datums_day_partitions, ignore_index=True)
            datums.append(datums_day_metadata)
        return datums

    @staticmethod
    def datum_tostring(datum_dict):
        return "{}.{}.{}.{}{}.{}={}{}".format(
            datum_dict["data_source"], datum_dict["data_metric"], datum_dict["data_type"], datum_dict["bin_width"], datum_dict["bin_timestamp"],
            datum_dict["bin_unit"].encode("utf-8"),
            int(datum_dict["data_value"]) / Decimal(datum_dict["data_scale"]),
            datum_dict["data_unit"].encode("utf-8")
        )

    def datum_get(self, datum_scope, data_metric, data_type, data_unit, bin_width, bin_unit, data_derived_period=None, data_derived_unit="day"):
        if data_metric in self.datums and data_type in self.datums[data_metric] and data_unit in self.datums[data_metric][data_type] and \
                        (str(bin_width) + bin_unit) in self.datums[data_metric][data_type][data_unit] and \
                        datum_scope in self.datums[data_metric][data_type][data_unit][str(bin_width) + bin_unit]:
            datum_dict = self.datums[data_metric][data_type][data_unit][str(bin_width) + bin_unit][datum_scope]
            if data_derived_period is None or (datum_dict["bin_width"] == data_derived_period and
                                                       datum_dict["bin_unit"] == data_derived_unit and
                                                           datum_dict["bin_timestamp"] %
                                                           Plugin.get_seconds(data_derived_period, data_derived_unit) == 0):
                return datum_dict
            else:
                return None

    def datums_filter_get(self, datums_filtered, datum_filter):
        log_timer = anode.Log(logging.DEBUG).start()
        for data_metric in self.datums:
            if Plugin.is_fitlered(datum_filter, "metrics", data_metric):
                for datum_type in self.datums[data_metric]:
                    if Plugin.is_fitlered(datum_filter, "types", datum_type):
                        for datum_unit in self.datums[data_metric][datum_type]:
                            if Plugin.is_fitlered(datum_filter, "units", datum_unit.encode("utf-8")):
                                for datum_bin in self.datums[data_metric][datum_type][datum_unit]:
                                    if Plugin.is_fitlered(datum_filter, "bins", datum_bin):
                                        datum_scopes = [DATUM_QUEUE_LAST] if "scope" not in datum_filter else datum_filter["scope"]
                                        for datum_scope in datum_scopes:
                                            if datum_scope in self.datums[data_metric][datum_type][datum_unit][datum_bin]:
                                                datums = []
                                                if datum_scope == DATUM_QUEUE_LAST:
                                                    datums_format = "dict"
                                                    datums = [self.datums[data_metric][datum_type][datum_unit][datum_bin][datum_scope]]
                                                elif datum_scope == DATUM_QUEUE_HISTORY:
                                                    self.datum_merge_buffer_history(self.datums[data_metric][datum_type][datum_unit][datum_bin]
                                                                                    [DATUM_QUEUE_BUFFER],
                                                                                    self.datums[data_metric][datum_type][datum_unit][datum_bin]
                                                                                    [DATUM_QUEUE_HISTORY])
                                                    datums_format = "df"
                                                    datums = self.datum_merge_history(
                                                        self.datums[data_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_HISTORY],
                                                        datum_filter)
                                                elif datum_scope == DATUM_QUEUE_PUBLISH:
                                                    datums_format = "avro"
                                                    datums = self.datums[data_metric][datum_type][datum_unit][datum_bin][datum_scope]
                                                if datums_format not in datums_filtered:
                                                    datums_filtered[datums_format] = []
                                                datums_filtered[datums_format].extend(datums)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.datums_filter_get)
        return datums_filtered

    @staticmethod
    def datums_filter(datums_filtered, datum_filter, datums):
        log_timer = anode.Log(logging.DEBUG).start()
        for datum in Plugin.datum_to_format(datums, "dict")["dict"]:
            if Plugin.is_fitlered(datum_filter, "metrics", datum["data_metric"]):
                if Plugin.is_fitlered(datum_filter, "types", datum["data_type"]):
                    if Plugin.is_fitlered(datum_filter, "units", datum["data_unit"].encode("utf-8")):
                        if Plugin.is_fitlered(datum_filter, "bins", str(datum["bin_width"]) + datum["bin_unit"]):
                            if "dict" not in datums_filtered:
                                datums_filtered["dict"] = []
                            datums_filtered["dict"].append(datum)
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_filter)
        return datums_filtered

    @staticmethod
    def is_fitlered(datum_filter, datum_filter_field, datum_field):
        if datum_filter_field not in datum_filter:
            return True
        for datum_filter_field_value in datum_filter[datum_filter_field]:
            if datum_field.lower().find(datum_filter_field_value.lower()) != -1:
                return True
        return False

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
    def datum_dict_to_avro(datum_dict):
        avro_writer = io.BytesIO()
        avro.io.DatumWriter(DATUM_SCHEMA_AVRO).write(datum_dict, avro.io.BinaryEncoder(avro_writer))
        return [avro_writer.getvalue()]

    @staticmethod
    def datum_dict_to_json(datum_dict):
        datum_dict = datum_dict.copy()
        if "anode_id" in datum_dict:
            datum_dict["anode_id"] = ID_HEX
        return [json.dumps(datum_dict, separators=(',', ':'))]

    @staticmethod
    def datum_dict_to_csv(datum_dict):
        datum_dict = datum_dict.copy()
        datum_dict["anode_id"] = ID_BASE64
        if datum_dict["data_unit"] not in DATUM_SCHEMA_TO_ASCII:
            DATUM_SCHEMA_TO_ASCII[datum_dict["data_unit"]] = urllib.quote_plus(datum_dict["data_unit"].encode("utf-8"))
        datum_dict["data_unit"] = DATUM_SCHEMA_TO_ASCII[datum_dict["data_unit"]]
        if datum_dict["bin_unit"] not in DATUM_SCHEMA_TO_ASCII:
            DATUM_SCHEMA_TO_ASCII[datum_dict["bin_unit"]] = urllib.quote_plus(datum_dict["bin_unit"].encode("utf-8"))
        datum_dict["bin_unit"] = DATUM_SCHEMA_TO_ASCII[datum_dict["bin_unit"]]
        return [','.join(str(datum_dict[datum_field["name"]]) for datum_field in DATUM_SCHEMA_JSON[7]["fields"])]

    @staticmethod
    def datum_dict_to_df(datum_dict):
        return Plugin.datums_dict_to_df([datum_dict])

    @staticmethod
    def datum_df_to_dict(datum_df):
        datums_dict = datum_df["data_df"].to_dict(orient="records")
        for datum_dict in datums_dict:
            datum_dict.update(datum_df)
            del datum_dict["data_df"]
        return datums_dict

    @staticmethod
    def datum_avro_to_dict(datum_avro):
        return [avro.io.DatumReader(DATUM_SCHEMA_AVRO).read(avro.io.BinaryDecoder(io.BytesIO(datum_avro)))]

    # noinspection PyUnusedLocal
    @staticmethod
    def datum_to_format(datums, datum_format, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        count = 0
        count_sum = sum(len(datums_values) for datums_values in datums.values())
        if "dict" in datums and len(datums["dict"]) > 0:
            datums["dict"] = Plugin.datums_sort(datums["dict"])
        log_timer.log("Plugin", "timer", lambda: "[*] count and sort for [{}] dict datums".format(len(datums["dict"])) if "dict" in datums else 0,
                      context=Plugin.datum_to_format, off_thread=off_thread)
        log_timer = anode.Log(logging.DEBUG).start()
        datums_formatted = {datum_format: []}
        datum_to_format_iterate = False
        for datums_format, datums_value in datums.iteritems():
            datums_format_function = None
            if datums_format == "dict":
                if datum_format == "avro":
                    datums_format_function = Plugin.datum_dict_to_avro
                elif datum_format == "json":
                    datums_format_function = Plugin.datum_dict_to_json
                elif datum_format == "csv":
                    datums_format_function = Plugin.datum_dict_to_csv
                elif datum_format == "df":
                    datums_format_function = Plugin.datum_dict_to_df
                elif datum_format != "dict":
                    raise ValueError("Unkown datum format conversion [{}] to [{}]".format(datums_format, datum_format))
            elif datums_format == "df" and datum_format == "dict":
                datums_format_function = Plugin.datum_df_to_dict
            elif datums_format != datum_format:
                datum_to_format_iterate = datum_format != "dict"
                if datums_format == "avro":
                    datums_format_function = Plugin.datum_avro_to_dict
                else:
                    raise ValueError("Unkown datum format conversion [{}] to [{}]".format(datums_format, datum_format))
            if "dict" if datum_to_format_iterate else datum_format not in datums_formatted:
                datums_formatted["dict" if datum_to_format_iterate else datum_format] = []
            if datums_format_function is None:
                datums_formatted["dict" if datum_to_format_iterate else datum_format].extend(datums_value)
                count += len(datums_value)
            else:
                for datum in datums_value:
                    datums_formatted["dict" if datum_to_format_iterate else datum_format].extend(datums_format_function(datum))
                    count += 1
                    if count % SERIALISATION_BATCH == 0 or count == count_sum:
                        if off_thread and count < count_sum:
                            log_timer.pause()
                            time.sleep(SERIALISATION_BATCH_SLEEP)
                            log_timer.start()
        log_timer.log("Plugin", "timer", lambda: "[*] {} to {} for [{}] datums".format(",".join(datums.keys()), datum_format, count),
                      context=Plugin.datum_to_format, off_thread=off_thread)
        return datums_formatted if not datum_to_format_iterate else Plugin.datum_to_format(datums_formatted, datum_format, off_thread)

    @staticmethod
    def datums_csv_to_dict(datums_csv):
        datums_dict = {"dict": []}
        for datum_dict in datums_csv:
            datum_dict["anode_id"] = base64.b64decode(datum_dict["anode_id"])
            datum_dict["data_value"] = long(datum_dict["data_value"])
            datum_dict["data_unit"] = HTMLParser.HTMLParser().unescape(datum_dict["data_unit"])
            if datum_dict["data_unit"] not in DATUM_SCHEMA_FROM_ASCII:
                DATUM_SCHEMA_FROM_ASCII[datum_dict["data_unit"]] = urllib.unquote_plus(datum_dict["data_unit"]).decode("utf-8")
            datum_dict["data_scale"] = float(datum_dict["data_scale"])
            datum_dict["data_timestamp"] = long(datum_dict["data_timestamp"])
            datum_dict["bin_timestamp"] = long(datum_dict["bin_timestamp"])
            datum_dict["bin_width"] = int(datum_dict["bin_width"])
            if datum_dict["bin_unit"] not in DATUM_SCHEMA_FROM_ASCII:
                DATUM_SCHEMA_FROM_ASCII[datum_dict["bin_unit"]] = urllib.unquote_plus(datum_dict["bin_unit"]).decode("utf-8")
            datums_dict["dict"].append(datum_dict)
        return datums_dict

    @staticmethod
    def datums_dict_to_json(datums_dict, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        count = 0
        datums_json_fragments = []
        for datum_dict in datums_dict:
            datums_json_fragments.append(Plugin.datum_dict_to_json(datum_dict)[0])
            count += 1
            if count % SERIALISATION_BATCH == 0 or count == len(datums_dict):
                datums_json_fragments = [",".join(datums_json_fragments)]
                if off_thread and count < len(datums_dict):
                    log_timer.pause()
                    time.sleep(SERIALISATION_BATCH_SLEEP)
                    log_timer.start()
        datums_json = "".join(["[", "" if len(datums_json_fragments) == 0 else datums_json_fragments[0], "]"])
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_dict_to_json, off_thread=off_thread)
        return datums_json

    @staticmethod
    def datums_dict_to_df(datums_dict, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        datums_df = []
        datums_data = {}
        datums_metadata = {}
        for datum_dict in datums_dict:
            datum_id = "_".join([datum_dict["data_metric"], datum_dict["data_type"], str(datum_dict["bin_width"]) + datum_dict["bin_unit"]])
            if datum_id not in datums_metadata:
                datum_metadata = datum_dict.copy()
                del datum_metadata["bin_timestamp"]
                del datum_metadata["data_timestamp"]
                del datum_metadata["data_value"]
                datums_metadata[datum_id] = datum_metadata
                datums_data[datum_id] = []
            datums_data[datum_id].append({"bin_timestamp": datum_dict["bin_timestamp"], "data_timestamp": datum_dict["data_timestamp"],
                                          "data_value": datum_dict["data_value"]})
        for datum_id, datum_metadata in datums_metadata.iteritems():
            datum_metadata["data_df"] = pandas.DataFrame(datums_data[datum_id])
            datums_df.append(datum_metadata)
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_dict_to_df, off_thread=off_thread)
        return datums_df

    @staticmethod
    def datums_df_to_csv(datums_df, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        datums_csv = datums_df.to_csv(index=False)
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_df_to_csv, off_thread=off_thread)
        return datums_csv

    @staticmethod
    def datums_df_to_df(datums_df, datum_options=None, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        datums_df_joined = []
        if len(datums_df) > 0:
            datums_df_groups = {}
            for datums_df_dict in datums_df:
                if datums_df_dict["data_unit"] not in DATUM_SCHEMA_TO_ASCII:
                    DATUM_SCHEMA_TO_ASCII[datums_df_dict["data_unit"]] = urllib.quote_plus(datums_df_dict["data_unit"].encode("utf-8"))
                if datums_df_dict["bin_unit"] not in DATUM_SCHEMA_TO_ASCII:
                    DATUM_SCHEMA_TO_ASCII[datums_df_dict["bin_unit"]] = urllib.quote_plus(datums_df_dict["bin_unit"].encode("utf-8"))
                datum_name = "&".join(
                    ["metrics=" + datums_df_dict["data_metric"],
                     "types=" + datums_df_dict["data_type"],
                     "bins=" + str(datums_df_dict["bin_width"]) + DATUM_SCHEMA_TO_ASCII[datums_df_dict["bin_unit"]],
                     "unit=" + DATUM_SCHEMA_TO_ASCII[datums_df_dict["data_unit"]],
                     "scale=" + str(datums_df_dict["data_scale"]),
                     "temporal=" + datums_df_dict["data_temporal"],
                     "source=" + datums_df_dict["data_source"],
                     "anode_id=" + ID_BASE64
                     ]
                )
                datums_df_reference = datums_df_dict["data_df"].copy(deep=False)
                datums_df_reference.set_index("bin_timestamp", inplace=True)
                datums_df_reference.index = pandas.to_datetime(datums_df_reference.index, unit="s")
                datums_df_reference["data_value"] = (datums_df_reference["data_value"] / datums_df_dict["data_scale"]).astype(
                    numpy.dtype(decimal.Decimal))
                datums_df_reference.rename(
                    columns={"data_timestamp": "data_timestamp@" + datum_name, "data_value": "data_value_scaled@" + datum_name},
                    inplace=True)
                if datum_name not in datums_df_groups:
                    datums_df_groups[datum_name] = datums_df_reference
                else:
                    datums_df_groups[datum_name] = pandas.concat([datums_df_groups[datum_name], datums_df_reference], ignore_index=True)
            datum_df = datums_df_groups.values()[0] if len(datums_df_groups) == 1 else \
                datums_df_groups.values()[0].join(datums_df_groups.values()[1:], how="outer")
            if datum_options is not None:
                if "period" in datum_options:
                    datum_df = getattr(datum_df.resample(str(datum_options["period"]) + "S"),
                                       "mean" if "method" not in datum_options else datum_options["method"])()
                if "fill" in datum_options:
                    if datum_options["fill"] == "all" or datum_options["fill"] == "linear":
                        datum_df[datums_dict_group_names] = datum_df[datums_dict_group_names].interpolate()
                    if datum_options["fill"] == "all" or datum_options["fill"] == "zeros":
                        datum_df[datums_dict_group_names] = datum_df[datums_dict_group_names].fillna(0)
            datum_df.index = datum_df.index.astype(numpy.int64) // 10 ** 9
            datum_df.index.name = "bin_timestamp"
            datum_df.reset_index(inplace=True)
            datum_df.reindex_axis(sorted(datum_df.columns), axis=1)
            datums_df_joined.append(datum_df)
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_df_to_df, off_thread=off_thread)
        return datums_df_joined

    @staticmethod
    def datums_to_format(datums, datum_format, datum_options, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        datums_count = 0
        for datums_format, datums_value in datums.iteritems():
            if datums_format == "df":
                datums_count += sum(len(datums_value_df["data_df"].index) for datums_value_df in datums_value)
            else:
                datums_count += len(datums_value)
        if datum_format == "json" or datum_format == "dict":
            datums_formatted = Plugin.datum_to_format(datums, "dict", off_thread)["dict"]
            if datum_format == "json":
                datums_formatted = Plugin.datums_dict_to_json(datums_formatted, off_thread)
        elif datum_format == "csv" or datum_format == "df":
            datums_formatted = Plugin.datums_df_to_df(Plugin.datum_to_format(datums, "df", off_thread)["df"], datum_options, off_thread=off_thread)
            if datum_format == "csv":
                datums_formatted = Plugin.datums_df_to_csv(datums_formatted[0], off_thread) if len(datums_formatted) > 0 else ""
        else:
            raise ValueError("Unkown datum format [{}]".format(datum_format))
        log_timer.log("Plugin", "timer", lambda: "[*] {} to {} for [{}] datums".format(",".join(datums.keys()), datum_format, datums_count),
                      context=Plugin.datums_to_format, off_thread=off_thread)
        return datums_formatted

    def datum_value(self, data, keys=None, default=None, factor=1):
        # noinspection PyBroadException
        try:
            value = data if keys is None else reduce(operator.getitem, keys, data)
            if isinstance(value, basestring) and not value:
                value = None
            if value is None:
                value = default
                anode.Log(logging.WARN).log("Plugin", "state",
                                            lambda: "[{}] setting value {} to default [{}] from response [{}]".format(self.name, keys, default, data))
            return value if not isinstance(value, numbers.Number) else int(value * factor)
        except Exception as exception:
            anode.Log(logging.ERROR).log("Plugin", "error",
                                         lambda: "[{}] setting value {} to default [{}] from response [{}] due to error [{}]".format(
                                             self.name, keys, default, data, exception), exception)
            return None if default is None else int(default * factor)

    def get_time(self):
        return calendar.timegm(time.gmtime()) if not self.is_clock else (int(self.reactor.seconds()) + self.time_boot)

    def get_time_period(self, timestamp, period):
        return (timestamp + self.time_tmz_offset) - (timestamp + self.time_tmz_offset) % period - self.time_tmz_offset

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
    def get(parent, module, config, reactor):
        plugin = getattr(import_module("anode.plugin") if hasattr(anode.plugin, module.title()) else
                         import_module("anode.plugin." + module), module.title())(parent, module, config, reactor)
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] initialised".format(module))
        return plugin

    __metaclass__ = abc.ABCMeta

    def __init__(self, parent, name, config, reactor):
        self.has_poll = getattr(self, "_poll", None) is not None
        self.has_push = getattr(self, "_push", None) is not None
        self.is_clock = isinstance(reactor, Clock)
        self.anode = parent
        self.name = name
        self.config = config
        self.reactor = reactor
        self.datums = {}
        self.datums_pushed = 0
        self.datums_dropped = 0
        self.datums_buffer = 0
        self.datums_history = 0
        self.datums_days = 0
        self.time_seen = None
        self.time_boot = calendar.timegm(time.gmtime())
        time_local = time.localtime()
        self.time_tmz_offset = calendar.timegm(time_local) - calendar.timegm(time.gmtime(time.mktime(time_local)))
        self.datums_buffer_batch = BUFFER_BATCH_DEFAULT if ("buffer_ticks" not in self.config or self.config["buffer_ticks"] < 1) \
            else self.config["buffer_ticks"]


BUFFER_BATCH_DEFAULT = 60

SERIALISATION_BATCH = 1000
SERIALISATION_BATCH_SLEEP = 0.3

DATUM_QUEUE_MIN = "min"
DATUM_QUEUE_MAX = "max"
DATUM_QUEUE_LAST = "last"
DATUM_QUEUE_PUBLISH = "publish"
DATUM_QUEUE_BUFFER = "buffer"
DATUM_QUEUE_HISTORY = "history"

DATUM_SCHEMA_TO_ASCII = {}
DATUM_SCHEMA_FROM_ASCII = {}
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
