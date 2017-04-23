from __future__ import print_function

import json
import logging

import HTMLParser
import StringIO
import abc
import avro.io
import avro.schema
import avro.schema
import base64
import calendar
import datetime
import decimal
import io
import matplotlib
import matplotlib.pyplot as plot
import numbers
import numpy
import operator
import os
import pandas
import re
import time
import urllib
from avro.io import AvroTypeException
from collections import deque
from cycler import cycler
from decimal import Decimal
from functools import reduce
from importlib import import_module
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from twisted.internet.task import Clock
from uuid import getnode as get_mac

import anode.plugin
import avro


# noinspection PyTypeChecker
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

    def repeat(self, force=False):
        log_timer = anode.Log(logging.DEBUG).start()
        for datum_metric in self.datums:
            for datum_type in self.datums[datum_metric]:
                for datum_unit in self.datums[datum_metric][datum_type]:
                    for datum_bin in self.datums[datum_metric][datum_type][datum_unit]:
                        datums = self.datums[datum_metric][datum_type][datum_unit][datum_bin]
                        if DATUM_QUEUE_LAST in datums and DATUM_QUEUE_HISTORY in datums:
                            datum = datums[DATUM_QUEUE_LAST]
                            if datum["data_temporal"] != "derived":
                                datum_bin_timestamp = self.get_time()
                                if force and "history_partition_seconds" in self.config and self.config["history_partition_seconds"] > 0:
                                    datum_bin_timestamp = self.get_time_period(datum_bin_timestamp, Plugin.get_seconds(
                                        self.config["history_partition_seconds"], "second"))
                                if force or ((24 * 60 * 60) > (datum_bin_timestamp - datum["bin_timestamp"]) >= (self.config["repeat_seconds"] - 5)):

                                    def expired_max_min(max_or_min):
                                        if max_or_min in self.datums[datum_metric][datum_type][datum_unit][datum_bin]:
                                            if force or datums[max_or_min]["bin_timestamp"] < \
                                                    self.get_time_period(self.get_time(), Plugin.get_seconds(datums[max_or_min]["bin_width"],
                                                                                                             datums[max_or_min]["bin_unit"])):
                                                return True
                                        return False

                                    datum_value = datum["data_value"]
                                    if force and "history_partition_seconds" in self.config and self.config["history_partition_seconds"] > 0 and \
                                                    Plugin.get_seconds(datum["bin_width"], datum["bin_unit"]) == \
                                                    Plugin.get_seconds(self.config["history_partition_seconds"], "second"):
                                        if datum["data_type"] == "integral":
                                            datum_value = 0
                                        elif datum["data_type"] == "forecast":
                                            datum_value = None

                                    if datum_value is not None:
                                        self.datum_push(
                                            datum["data_metric"],
                                            "forecast" if datum["data_temporal"] == "forecast" else "repeat", datum["data_type"],
                                            datum_value,
                                            datum["data_unit"],
                                            datum["data_scale"],
                                            datum["data_timestamp"],
                                            datum_bin_timestamp,
                                            datum["bin_width"],
                                            datum["bin_unit"],
                                            data_string=datum["data_string"] if "data_string" in datum else None,
                                            data_derived_max=expired_max_min(DATUM_QUEUE_MAX),
                                            data_derived_min=expired_max_min(DATUM_QUEUE_MIN),
                                            data_derived_force=force,
                                            data_push_force=True
                                        )
        self.publish()
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.repeat)

    def publish(self):
        matric_name = "anode." + self.name + "."
        time_now = self.get_time()
        metrics_count = sum(len(units)
                            for metrics in self.datums.values()
                            for types in metrics.values()
                            for units in types.values())
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
            data_transient=True
        )
        datums_buffer_count = sum(len(bins[DATUM_QUEUE_BUFFER])
                                  for metrics in self.datums.values()
                                  for types in metrics.values()
                                  for units in types.values()
                                  for bins in units.values())
        self.datum_push(
            matric_name + "buffer",
            "current", "point",
            self.datum_value(0 if metrics_count == 0 else (float(datums_buffer_count) / (self.datums_buffer_batch * metrics_count) * 100)),
            "%",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_upper=100,
            data_bound_lower=0,
            data_transient=True
        )
        datums_count = sum(0 if DATUM_QUEUE_HISTORY not in bins else (
            sum(len(partitions["data_df"].index) for partitions in bins[DATUM_QUEUE_HISTORY].values()))
                           for metrics in self.datums.values()
                           for types in metrics.values()
                           for units in types.values()
                           for bins in units.values())
        self.datum_push(
            matric_name + "history",
            "current", "point",
            self.datum_value(0 if ("history_ticks" not in self.config or self.config["history_ticks"] < 1) else (
                float(datums_count) / self.config["history_ticks"] * 100)),
            "%",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_upper=100,
            data_bound_lower=0,
            data_transient=True
        )
        partitions_count_max = max(0 if DATUM_QUEUE_HISTORY not in bins else (
            len(bins[DATUM_QUEUE_HISTORY]))
                                   for metrics in self.datums.values()
                                   for types in metrics.values()
                                   for units in types.values()
                                   for bins in units.values())
        self.datum_push(
            matric_name + "partitions",
            "current", "point",
            self.datum_value(0 if ("history_partitions" not in self.config or self.config["history_partitions"] < 1) else (
                float(partitions_count_max) / self.config["history_partitions"] * 100)),
            "%",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_upper=100,
            data_bound_lower=0,
            data_transient=True
        )
        self.datum_push(
            matric_name + "up-time",
            "current", "point",
            self.datum_value((time_now - self.time_boot) / Decimal(24 * 60 * 60), factor=100),
            "d",
            100,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_lower=0,
            data_transient=True
        )
        self.datum_push(
            matric_name + "last-seen",
            "current", "point",
            self.datum_value(self.time_seen),
            "scalor",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_transient=True

        )
        datums_publish_pending = 0
        publish_service = self.config["publish_service"] \
            if ("publish_service" in self.config and "publish_upstream" in self.config and self.config["publish_upstream"]) else None
        for datum_metric in self.datums:
            for datum_type in self.datums[datum_metric]:
                for datum_unit in self.datums[datum_metric][datum_type]:
                    for datum_bin in self.datums[datum_metric][datum_type][datum_unit]:
                        datums_publish = self.datums[datum_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_PUBLISH]
                        datums_publish_len = len(datums_publish)
                        if publish_service is not None and publish_service.isConnected():
                            for index in xrange(datums_publish_len):
                                datum_avro = datums_publish.popleft()
                                anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] publishing datum [{}] datum [{}] of [{}]".format(
                                    self.name, self.datum_tostring(self.datum_avro_to_dict(datum_avro)[0]), index + 1, datums_publish_len))
                                publish_service.publishMessage(datum_avro, datums_publish, lambda failure, message, queue: (
                                    anode.Log(logging.WARN).log("Plugin", "state", lambda: "[{}] publish failed datum [{}] with reason {}".format(
                                        self.name, self.datum_tostring(self.datum_avro_to_dict(datum_avro)[0]), str(failure).replace("\n", ""))),
                                    queue.appendleft(message)))
                        elif publish_service is None:
                            datums_publish.clear()
                        datums_publish_pending += len(datums_publish)
        self.datum_push(
            matric_name + "queue",
            "current", "point",
            self.datum_value(datums_publish_pending),
            "datums",
            1,
            time_now,
            time_now,
            self.config["poll_seconds"],
            "second",
            data_bound_lower=0,
            data_transient=True
        )
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] published [{}] datums".format(self.name, datums_publish_pending))

    def datum_push(self, data_metric, data_temporal, data_type, data_value, data_unit, data_scale, data_timestamp, bin_timestamp, bin_width,
                   bin_unit, data_string=None, data_bound_upper=None, data_bound_lower=None, data_derived_max=False, data_derived_min=False,
                   data_derived_period=1, data_derived_unit="day", data_derived_force=False, data_push_force=False, data_transient=False):
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
                "data_string": data_string,
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
                                             lambda: "[{}] error serialising Avro object [{}]".format(self.name, datum_dict), exception)
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
                    DATUM_QUEUE_BUFFER: deque()
                }
                if not data_transient:
                    self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][datum_dict["data_unit"]][
                        str(datum_dict["bin_width"]) + datum_dict["bin_unit"]][DATUM_QUEUE_HISTORY] = {}
            datums_deref = self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][datum_dict["data_unit"]][str(datum_dict["bin_width"]) +
                                                                                                                    datum_dict["bin_unit"]]
            if DATUM_QUEUE_LAST not in datums_deref or datums_deref[DATUM_QUEUE_LAST]["data_value"] != datum_dict["data_value"] or \
                            datums_deref[DATUM_QUEUE_LAST]["data_unit"] != datum_dict["data_unit"] or \
                            datums_deref[DATUM_QUEUE_LAST]["data_scale"] != datum_dict["data_scale"] or \
                    ("data_string" in datums_deref[DATUM_QUEUE_LAST] and
                             datums_deref[DATUM_QUEUE_LAST]["data_string"] != datum_dict["data_string"]) or \
                    ("repeat_partition" in self.config and self.config["repeat_partition"] and
                             self.get_time_period(datums_deref[DATUM_QUEUE_LAST]["bin_timestamp"], self.config["history_partition_seconds"]) !=
                             self.get_time_period(datum_dict["bin_timestamp"], self.config["history_partition_seconds"])) or \
                    data_push_force:
                self.time_seen = self.get_time()
                datums_deref[DATUM_QUEUE_LAST] = datum_dict
                bin_timestamp_derived = self.get_time_period(datum_dict["bin_timestamp"],
                                                             Plugin.get_seconds(data_derived_period, data_derived_unit))
                if not data_transient:
                    if data_derived_max:
                        if DATUM_QUEUE_MAX not in datums_deref or datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] < bin_timestamp_derived or \
                                        datums_deref[DATUM_QUEUE_MAX]["data_value"] < datum_dict["data_value"] or data_derived_force:
                            datums_deref[DATUM_QUEUE_MAX] = datum_dict.copy()
                            datums_deref[DATUM_QUEUE_MAX]["bin_type"] = "high"
                            datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"] = bin_timestamp_derived
                            datums_deref[DATUM_QUEUE_MAX]["bin_width"] = data_derived_period
                            datums_deref[DATUM_QUEUE_MAX]["bin_unit"] = data_derived_unit
                            anode.Log(logging.DEBUG).log("Plugin", "state",
                                                         lambda: "[{}] seleted high [{}]".format(self.name,
                                                                                                 self.datum_tostring(datums_deref[DATUM_QUEUE_MAX])))
                            self.datum_push(data_metric, "derived", "high", datum_dict["data_value"], data_unit, data_scale,
                                            datum_dict["data_timestamp"], datums_deref[DATUM_QUEUE_MAX]["bin_timestamp"],
                                            datum_dict["bin_width"] if datum_dict["data_type"] == "integral"
                                            else data_derived_period, datum_dict["bin_unit"] if datum_dict["data_type"] == "integral"
                                            else data_derived_unit, data_derived_force=data_derived_force, data_push_force=data_push_force,
                                            data_transient=data_transient)
                    if data_derived_min:
                        if DATUM_QUEUE_MIN not in datums_deref or datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"] < bin_timestamp_derived or \
                                        datums_deref[DATUM_QUEUE_MIN]["data_value"] > datum_dict["data_value"] or data_derived_force:
                            datums_deref[DATUM_QUEUE_MIN] = datum_dict.copy()
                            datums_deref[DATUM_QUEUE_MIN]["bin_type"] = "low"
                            datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"] = bin_timestamp_derived
                            datums_deref[DATUM_QUEUE_MIN]["bin_width"] = data_derived_period
                            datums_deref[DATUM_QUEUE_MIN]["bin_unit"] = data_derived_unit
                            anode.Log(logging.DEBUG).log("Plugin", "state",
                                                         lambda: "[{}] deleted low [{}]".format(self.name,
                                                                                                self.datum_tostring(datums_deref[DATUM_QUEUE_MIN])))
                            self.datum_push(data_metric, "derived", "low", datum_dict["data_value"], data_unit, data_scale,
                                            datum_dict["data_timestamp"], datums_deref[DATUM_QUEUE_MIN]["bin_timestamp"],
                                            datum_dict["bin_width"] if datum_dict["data_type"] == "integral"
                                            else data_derived_period, datum_dict["bin_unit"] if datum_dict["data_type"] == "integral"
                                            else data_derived_unit, data_derived_force=data_derived_force, data_push_force=data_push_force,
                                            data_transient=data_transient)
                    datums_deref[DATUM_QUEUE_PUBLISH].append(datum_avro)
                    if "history_ticks" in self.config and self.config["history_ticks"] > 0 and \
                                    "history_partitions" in self.config and self.config["history_partitions"] > 0 and \
                                    "history_partition_seconds" in self.config and self.config["history_partition_seconds"] > 0:
                        bin_timestamp_partition = self.get_time_period(datum_dict["bin_timestamp"], self.config["history_partition_seconds"])
                        if len(datums_deref[DATUM_QUEUE_BUFFER]) == self.datums_buffer_batch or bin_timestamp_partition \
                                not in datums_deref[DATUM_QUEUE_HISTORY]:
                            self.datum_merge_buffer_history(datums_deref[DATUM_QUEUE_BUFFER], datums_deref[DATUM_QUEUE_HISTORY])
                        datums_deref[DATUM_QUEUE_BUFFER].append(datum_dict)
                        anode.Log(logging.DEBUG).log("Plugin", "state",
                                                     lambda: "[{}] saved datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
                anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] pushed datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
                self.anode.push_datums({"dict": [datum_dict]})
            else:
                anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] dropped datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.datum_push)

    def datum_merge_buffer_history(self, datums_buffer, datums_history):
        log_timer = anode.Log(logging.DEBUG).start()
        if "history_ticks" in self.config and self.config["history_ticks"] > 0 and \
                        "history_partitions" in self.config and self.config["history_partitions"] > 0 and \
                        "history_partition_seconds" in self.config and self.config["history_partition_seconds"] > 0:
            bin_timestamp_partition_max = None
            if len(datums_buffer) > 0:
                datums_buffer_partition = {}
                for datum in datums_buffer:
                    bin_timestamp_partition = self.get_time_period(datum["bin_timestamp"], self.config["history_partition_seconds"])
                    bin_timestamp_partition_max = bin_timestamp_partition if \
                        (bin_timestamp_partition_max is None or bin_timestamp_partition_max < bin_timestamp_partition) else \
                        bin_timestamp_partition_max
                    if bin_timestamp_partition not in datums_buffer_partition:
                        datums_buffer_partition[bin_timestamp_partition] = []
                    datums_buffer_partition[bin_timestamp_partition].append(datum)
                for bin_timestamp_partition, datums in datums_buffer_partition.iteritems():
                    datums_df = self.datums_dict_to_df(datums_buffer)
                    if len(datums_df) != 1:
                        raise ValueError("Assertion error merging mixed datum types when there should not be any!")
                    datums_df = datums_df[0]
                    if bin_timestamp_partition not in datums_history:
                        datums_history[bin_timestamp_partition] = datums_df
                    else:
                        datums_history[bin_timestamp_partition]["data_df"] = pandas.concat([datums_history[bin_timestamp_partition]["data_df"],
                                                                                            datums_df["data_df"]], ignore_index=True)
                        anode.Log(logging.DEBUG).log("Plugin", "state",
                                                     lambda: "[{}] merged buffer partition [{}]".format(self.name, bin_timestamp_partition))
                datums_buffer.clear()
            if len(datums_history) > 0 and bin_timestamp_partition_max is not None:
                bin_timestamp_partition_lower = bin_timestamp_partition_max - \
                                                (self.config["history_partitions"] - 1) * self.config["history_partition_seconds"]
                for bin_timestamp_partition_del in datums_history.keys():
                    if bin_timestamp_partition_del < bin_timestamp_partition_lower:
                        del datums_history[bin_timestamp_partition_del]
                        anode.Log(logging.DEBUG).log("Plugin", "state",
                                                     lambda: "[{}] purged expired partition [{}]".format(self.name, bin_timestamp_partition_del))
            while len(datums_history) > self.config["history_partitions"] or \
                            sum(len(datums_df_cached["data_df"].index) for datums_df_cached in datums_history.itervalues()) > \
                            self.config["history_ticks"]:
                bin_timestamp_partition_del = min(datums_history.keys())
                del datums_history[bin_timestamp_partition_del]
                anode.Log(logging.DEBUG).log("Plugin", "state",
                                             lambda: "[{}] purged upperbounded partition [{}]".format(self.name,
                                                                                                      bin_timestamp_partition_del))
        log_timer.log("Plugin", "timer", lambda: "[{}] partitions [{}]".format(self.name, len(datums_history)),
                      context=self.datum_merge_buffer_history)

    def datum_merge_history(self, datums_history, datum_filter):
        log_timer = anode.Log(logging.DEBUG).start()
        datums = []
        if "history_ticks" in self.config and self.config["history_ticks"] > 0 and \
                        "history_partitions" in self.config and self.config["history_partitions"] > 0 and \
                        "history_partition_seconds" in self.config and self.config["history_partition_seconds"] > 0:
            datums_partitions = []
            if "partitions" in datum_filter:
                datum_filter_partitions = 0 if not min(datum_filter["partitions"]).isdigit() else int(min(datum_filter["partitions"]))
                if datum_filter_partitions > 0 and len(datums_history) > 0:
                    for datums_partition in sorted(datums_history.iterkeys())[(
                            (len(datums_history) - datum_filter_partitions) if len(datums_history) > datum_filter_partitions else 0):]:
                        datums_partitions.append(datums_history[datums_partition]["data_df"])
            else:
                datums_partition_lower = DATUM_TIMESTAMP_MIN if "start" not in datum_filter else \
                    (self.get_time_period(int(min(datum_filter["start"])), self.config["history_partition_seconds"]))
                datums_partition_upper = DATUM_TIMESTAMP_MAX if "finish" not in datum_filter else \
                    (self.get_time_period(int(max(datum_filter["finish"])), self.config["history_partition_seconds"]))
                for datums_partition in sorted(datums_history.keys()):
                    if datums_partition_upper >= datums_partition >= datums_partition_lower:
                        datums_partitions.append(datums_history[datums_partition]["data_df"])
            if len(datums_partitions) > 0:
                datums_partition_metadata = datums_history.itervalues().next().copy()
                if len(datums_partitions) == 1:
                    datums_partition_metadata["data_df"] = datums_partitions[0].copy(deep=False)
                else:
                    datums_partition_metadata["data_df"] = pandas.concat(datums_partitions, ignore_index=True)
                datums_partition_metadata["data_df"] = Plugin.datums_df_resample(
                    Plugin.datums_df_filter(Plugin.datums_df_reindex(datums_partition_metadata["data_df"]), datum_filter), datum_filter)
                datums.append(datums_partition_metadata)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.datum_merge_history)
        return datums

    @staticmethod
    def datum_tostring(datum_dict):
        return "{}.{}.{}.{}{}.{}={}{}{}".format(
            datum_dict["data_source"], datum_dict["data_metric"], datum_dict["data_type"], datum_dict["bin_width"], datum_dict["bin_timestamp"],
            datum_dict["bin_unit"].encode("utf-8"),
            int(datum_dict["data_value"]) / Decimal(datum_dict["data_scale"]),
            datum_dict["data_unit"].encode("utf-8"),
            datum_dict["data_string"] if datum_dict["data_string"] is not None else ""
        )

    def datum_get(self, datum_scope, data_metric, data_type, data_unit, bin_width, bin_unit, data_derived_period=None, data_derived_unit="day"):
        if data_metric in self.datums and data_type in self.datums[data_metric] and data_unit in self.datums[data_metric][data_type] and \
                        (str(bin_width) + bin_unit) in self.datums[data_metric][data_type][data_unit] and \
                        datum_scope in self.datums[data_metric][data_type][data_unit][str(bin_width) + bin_unit]:
            datum_dict = self.datums[data_metric][data_type][data_unit][str(bin_width) + bin_unit][datum_scope]
            return datum_dict if (data_derived_period is None or datum_dict["bin_timestamp"] ==
                                  self.get_time_period(self.get_time(), Plugin.get_seconds(data_derived_period,
                                                                                           data_derived_unit))) else None
        return None

    def datums_filter_get(self, datums_filtered, datum_filter):
        log_timer = anode.Log(logging.DEBUG).start()
        for datum_metric in self.datums:
            if Plugin.is_fitlered(datum_filter, "metrics", datum_metric):
                for datum_type in self.datums[datum_metric]:
                    if Plugin.is_fitlered(datum_filter, "types", datum_type):
                        for datum_unit in self.datums[datum_metric][datum_type]:
                            if Plugin.is_fitlered(datum_filter, "units", datum_unit.encode("utf-8"), exact_match=True):
                                for datum_bin in self.datums[datum_metric][datum_type][datum_unit]:
                                    if Plugin.is_fitlered(datum_filter, "bins", datum_bin):
                                        datum_scopes = [DATUM_QUEUE_LAST] if "scope" not in datum_filter else datum_filter["scope"]
                                        for datum_scope in datum_scopes:
                                            if datum_scope in self.datums[datum_metric][datum_type][datum_unit][datum_bin]:
                                                datums = []
                                                if datum_scope == DATUM_QUEUE_LAST:
                                                    datums_format = "dict"
                                                    datums = [self.datums[datum_metric][datum_type][datum_unit][datum_bin][datum_scope]]
                                                elif datum_scope == DATUM_QUEUE_HISTORY:
                                                    self.datum_merge_buffer_history(self.datums[datum_metric][datum_type][datum_unit][datum_bin]
                                                                                    [DATUM_QUEUE_BUFFER],
                                                                                    self.datums[datum_metric][datum_type][datum_unit][datum_bin]
                                                                                    [DATUM_QUEUE_HISTORY])
                                                    datums_format = "df"
                                                    datums = self.datum_merge_history(
                                                        self.datums[datum_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_HISTORY],
                                                        datum_filter)
                                                elif datum_scope == DATUM_QUEUE_PUBLISH:
                                                    datums_format = "avro"
                                                    datums = self.datums[datum_metric][datum_type][datum_unit][datum_bin][datum_scope]
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
                    if Plugin.is_fitlered(datum_filter, "units", datum["data_unit"].encode("utf-8"), exact_match=True):
                        if Plugin.is_fitlered(datum_filter, "bins", str(datum["bin_width"]) + datum["bin_unit"]):
                            if "dict" not in datums_filtered:
                                datums_filtered["dict"] = []
                            datums_filtered["dict"].append(datum)
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_filter)
        return datums_filtered

    @staticmethod
    def is_fitlered(datum_filter, datum_filter_field, datum_field, exact_match=False):
        if datum_filter_field not in datum_filter:
            return True
        for datum_filter_field_value in datum_filter[datum_filter_field]:
            if exact_match and datum_field.lower() == datum_filter_field_value.lower() or \
                            not exact_match and datum_field.lower().find(datum_filter_field_value.lower()) != -1:
                return True
        return False

    @staticmethod
    def datums_sort(datums):
        return sorted(datums, key=lambda datum: (
            "aaaaa" if datum["data_unit"] == "$" else
            "bbbbb" if datum["data_unit"] == "W" else
            "zzzzz" + datum["data_unit"],
            datum["data_metric"],
            "aaaaa" if datum["data_type"] == "point" else
            "bbbbb" if datum["data_type"] == "mean" else
            "ccccc" if datum["data_type"] == "integral" else
            "ddddd" if datum["data_type"] == "low" else
            "eeeee" if datum["data_type"] == "high" else
            datum["data_type"],
            "aaaaa" if datum["bin_unit"] == "second" else
            "bbbbb" if datum["bin_unit"] == "minute" else
            "ccccc" if datum["bin_unit"] == "hour" else
            "ddddd" if datum["bin_unit"] == "day-time" else
            "eeeee" if datum["bin_unit"] == "night-time" else
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
        return [','.join(str(datum_dict[datum_field]) for datum_field in DATUM_SCHEMA_MODEL.iterkeys())]

    @staticmethod
    def datum_dict_to_df(datum_dict):
        return Plugin.datums_dict_to_df([datum_dict])

    @staticmethod
    def datum_df_to_dict(datum_df):
        datums_dict_df = Plugin.datums_df_unindex(datum_df["data_df"].copy(deep=False))
        datums_dict = datums_dict_df.to_dict(orient="records")
        for datum_dict in datums_dict:
            datum_dict["data_value"] = numpy.asscalar(datum_dict["data_value"])
            datum_dict["bin_timestamp"] = numpy.asscalar(datum_dict["bin_timestamp"])
            datum_dict["data_timestamp"] = numpy.asscalar(datum_dict["data_timestamp"])
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

    # noinspection PyArgumentList
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
    def datums_df_to_svg(datums_df, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        datums_plot_font_size = 14
        datums_plot_alpha = 0.7
        datums_plot_colour = "white"
        datums_plot_colour_foreground = "0.5"
        datums_plot_colour_background = "black"
        datums_plot_colour_lines = ["yellow", "lime", "red", "orange", "magenta", "lightsteelblue", "cyan", "bisque", "blue"]
        datums_plot_title = datums_df.title if hasattr(datums_df, "title") else None
        datums_plot_buffer = StringIO.StringIO()
        datums_df = Plugin.datums_df_reindex(datums_df)
        datums_points = len(datums_df.index)
        datums_axes_x_range = ((datums_df.index[-1] - datums_df.index[0]).total_seconds()) if datums_points > 0 else 0
        if datums_points == 0 or datums_axes_x_range == 0:
            return SVG_EMPTY
        datums_figure = Figure()
        datums_axes = datums_figure.add_subplot(111)
        datums_axes.set_prop_cycle(cycler("color", datums_plot_colour_lines))
        for column in datums_df:
            datums_axes.plot(datums_df.index, datums_df[column])
        datums_axes.margins(0, 0, tight=True)
        datums_axes.minorticks_off()
        if datums_axes_x_range <= (10 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.SecondLocator(interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M:%S"))
        elif datums_axes_x_range <= (60 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.SecondLocator(bysecond=[0, 10, 20, 30, 40, 50], interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M:%S"))
        if datums_axes_x_range <= (60 * 2 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
        if datums_axes_x_range <= (60 * 10 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55], interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
        elif datums_axes_x_range <= (60 * 60 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=[0, 10, 20, 30, 40, 50], interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
        elif datums_axes_x_range <= (60 * 60 * 4 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.MinuteLocator(byminute=[0, 30], interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
        elif datums_axes_x_range <= (60 * 60 * 8 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.HourLocator(interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
        elif datums_axes_x_range <= (60 * 60 * 16 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.HourLocator(byhour=[0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22], interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
        elif datums_axes_x_range <= (60 * 60 * 24 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.HourLocator(byhour=[0, 4, 8, 12, 16, 20], interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
        elif datums_axes_x_range <= (60 * 60 * 24 * 3 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.HourLocator(byhour=[0, 8, 16], interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%H:%M"))
        elif datums_axes_x_range <= (60 * 60 * 24 * 14 + 2):
            datums_axes.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=1))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%a"))
        else:
            datums_axes.xaxis.set_major_locator(matplotlib.dates.DayLocator(interval=3))
            datums_axes.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%a"))
        datums_axes.xaxis.label.set_visible(False)
        datums_axes.xaxis.label.set_color(datums_plot_colour)
        datums_axes.tick_params(axis="x", colors=datums_plot_colour)
        datums_axes.yaxis.tick_right()
        datums_axes.yaxis.label.set_color(datums_plot_colour)
        datums_axes.yaxis.grid(b=True, which="major", color=datums_plot_colour_foreground, linestyle='--')
        datums_axes.tick_params(axis="y", colors=datums_plot_colour)
        datums_axes.spines["bottom"].set_color(datums_plot_colour)
        datums_axes.spines["top"].set_color(datums_plot_colour)
        datums_axes.spines["left"].set_color(datums_plot_colour)
        datums_axes.spines["right"].set_color(datums_plot_colour)
        datums_axes.patch.set_facecolor(datums_plot_colour_background)
        if datums_plot_title is not None and len(datums_plot_title) > 20:
            datums_plot_legend = datums_axes.legend(loc="lower left", ncol=1)
        else:
            datums_plot_legend = datums_axes.legend(loc="upper left", ncol=1)
        for datums_plot_legend_text in datums_plot_legend.get_texts():
            datums_plot_legend_text.set_fontsize(datums_plot_font_size)
            datums_plot_legend_text.set_color(datums_plot_colour)
        datums_plot_legend.get_frame().set_alpha(datums_plot_alpha)
        datums_plot_legend.get_frame().set_edgecolor(datums_plot_colour_foreground)
        datums_plot_legend.get_frame().set_facecolor(datums_plot_colour_background)
        datums_figure.subplots_adjust(left=0, right=0.9, top=0.9725, bottom=0.05)
        datums_canvas = FigureCanvas(datums_figure)
        datums_canvas.draw()
        if datums_plot_title is not None:
            datums_axes.text(0.98, 0.975, datums_plot_title, horizontalalignment="right", verticalalignment="top", transform=datums_axes.transAxes,
                             color=datums_plot_colour, fontsize=datums_plot_font_size,
                             bbox=dict(facecolor=datums_plot_colour_background, edgecolor=datums_plot_colour_background, alpha=datums_plot_alpha,
                                       boxstyle="round,pad=0.2"))
        datums_axes_x_labels = [tick.get_text() for tick in datums_axes.get_xticklabels()]
        if len(datums_axes_x_labels) > 1:
            datums_axes_x_labels[0] = u""
        datums_axes.set_xticklabels(datums_axes_x_labels)
        datums_axes.set_ylim([datums_axes.get_ylim()[0] * 0.9, datums_axes.get_ylim()[1] * 1.15])
        datums_canvas.print_figure(datums_plot_buffer,
                                   facecolor=datums_plot_colour_background,
                                   figsize=(8.68, 12),
                                   dpi=1200,
                                   format="svg")
        datums_figure.clf()
        plot.close()
        del datums_canvas
        del datums_figure
        del datums_df
        datums_plot_buffer.seek(0)
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_df_to_svg, off_thread=off_thread)
        return datums_plot_buffer.buf

    @staticmethod
    def datums_df_to_df(datums_df, datum_options=None, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        datums_df_df = []
        if len(datums_df) > 0:
            log_timer_input = anode.Log(logging.DEBUG).start()
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
                ).decode("utf-8")
                if datum_name not in datums_df_groups:
                    datums_df_groups[datum_name] = datums_df_dict.copy()
                    datums_df_groups[datum_name]["data_df"] = [datums_df_dict["data_df"]]
                else:
                    datums_df_groups[datum_name]["data_df"].append(datums_df_dict["data_df"])
            log_timer_input.log("Plugin", "timer", lambda: "[*] input [{}] dataframes and [{}] datums".format(
                len(datums_df),
                sum(len(datums_df_value["data_df"].index) for datums_df_value in datums_df)), context=Plugin.datums_df_to_df, off_thread=off_thread)
            log_timer_intermediate = anode.Log(logging.DEBUG).start()
            datums_df_groups_concat = {}
            datums_df_groups_concat_data = []
            for datum_name, datums_df_dict in datums_df_groups.iteritems():
                datums_df_dict_df = pandas.concat(datums_df_dict["data_df"])
                datums_df_dict_df = Plugin.datums_df_reindex(datums_df_dict_df)
                datums_df_dict_df["data_value"] = (datums_df_dict_df["data_value"] / datums_df_dict["data_scale"]).astype(
                    numpy.dtype(decimal.Decimal))
                datums_df_dict_df.rename(columns={"data_timestamp": "data_timestamp@" + datum_name, "data_value": "data_value_scaled@" + datum_name},
                                         inplace=True)
                datums_df_groups_concat_data.append("data_value_scaled@" + datum_name)
                datums_df_groups_concat[datum_name] = datums_df_dict_df
            datum_df = pandas.concat(datums_df_groups_concat.values(), axis=1, join="outer")
            log_timer_intermediate.log("Plugin", "timer", lambda: "[*] intermediate [{}] dataframes and [{}] datums".format(
                len(datums_df_groups_concat),
                sum(len(datums_df_groups_concat_df.index) for datums_df_groups_concat_df in datums_df_groups_concat.values())),
                                       context=Plugin.datums_df_to_df, off_thread=off_thread)
            log_timer_output = anode.Log(logging.DEBUG).start()
            datum_df = Plugin.datums_df_unindex(
                Plugin.datums_df_fill(
                    Plugin.datums_df_resample(datum_df, datum_options), datums_df_groups_concat_data, datum_options))
            if "print" in datum_options and datum_options["print"][0] == "pretty":
                datum_df = Plugin.datums_df_metadata_pretty(Plugin.datums_df_data_pretty(datum_df))
            datums_df_df.append(datum_df)
            log_timer_output.log("Plugin", "timer", lambda: "[*] output [{}] dataframes and [{}] datums".format(
                len(datums_df_df),
                sum(response_df[response_df_column].count() for response_df in datums_df_df for response_df_column in response_df if
                    response_df_column.startswith("data_value"))),
                                 context=Plugin.datums_df_to_df, off_thread=off_thread)
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_df_to_df, off_thread=off_thread)
        return datums_df_df

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
        elif datum_format == "csv" or datum_format == "svg" or datum_format == "df":
            datums_formatted = Plugin.datums_df_to_df(Plugin.datum_to_format(datums, "df", off_thread)["df"], datum_options, off_thread=off_thread)
            if datum_format == "csv":
                datums_formatted = Plugin.datums_df_to_csv(datums_formatted[0], off_thread) if len(datums_formatted) > 0 else ""
            elif datum_format == "svg":
                datums_formatted = Plugin.datums_df_to_svg(datums_formatted[0], off_thread) if len(datums_formatted) > 0 else SVG_EMPTY
        else:
            raise ValueError("Unkown datum format [{}]".format(datum_format))
        log_timer.log("Plugin", "timer", lambda: "[*] {} to {} for [{}] datums".format(",".join(datums.keys()), datum_format, datums_count),
                      context=Plugin.datums_to_format, off_thread=off_thread)
        return datums_formatted

    @staticmethod
    def datums_df_title(datum_df, datum_df_title=None):
        if datum_df_title is not None:
            datum_df.title = datum_df_title
        return datum_df_title if datum_df_title is not None else (datum_df.title if hasattr(datum_df, "title") else None)

    @staticmethod
    def datums_df_reindex(datum_df):
        datum_df_title = Plugin.datums_df_title(datum_df)
        if "Time" in datum_df:
            datum_df["bin_timestamp"] = pandas.to_datetime(datum_df["Time"])
            datum_df["bin_timestamp"] = (datum_df['bin_timestamp'] - datetime.datetime(1970, 1, 1)).dt.total_seconds()
            del datum_df["Time"]
        if "bin_timestamp" in datum_df:
            datum_df = datum_df[~datum_df["bin_timestamp"].duplicated(keep="first")]
            datum_df.set_index("bin_timestamp", inplace=True)
            datum_df.index = pandas.to_datetime(datum_df.index, unit="s")
        Plugin.datums_df_title(datum_df, datum_df_title)
        return datum_df

    @staticmethod
    def datums_df_unindex(datum_df):
        datum_df_title = Plugin.datums_df_title(datum_df)
        if "bin_timestamp" not in datum_df:
            datum_df.index = datum_df.index.astype(numpy.int64) // 10 ** 9
            datum_df.index.name = "bin_timestamp"
            datum_df.reset_index(inplace=True)
            datum_df.reindex_axis(sorted(datum_df.columns), axis=1)
        Plugin.datums_df_title(datum_df, datum_df_title)
        return datum_df

    @staticmethod
    def datums_df_filter(datum_df, datum_options):
        datum_df_title = Plugin.datums_df_title(datum_df)
        if datum_options is not None:
            datum_df = Plugin.datums_df_reindex(datum_df)
            if "start" in datum_options:
                datum_df = datum_df[datum_df.index >= pandas.to_datetime(datum_options["start"], unit="s")[0]]
            if "finish" in datum_options:
                datum_df = datum_df[datum_df.index <= pandas.to_datetime(datum_options["finish"], unit="s")[0]]
        Plugin.datums_df_title(datum_df, datum_df_title)
        return datum_df

    @staticmethod
    def datums_df_resample(datum_df, datum_options):
        datum_df_title = Plugin.datums_df_title(datum_df)
        if datum_options is not None:
            if "period" in datum_options:
                datum_df = getattr(datum_df.resample(str(datum_options["period"][0]) + "S"),
                                   "max" if "method" not in datum_options else datum_options["method"][0])()
        Plugin.datums_df_title(datum_df, datum_df_title)
        return datum_df

    @staticmethod
    def datums_df_fill(datum_df, datum_df_columns, datum_options):
        datum_df_title = Plugin.datums_df_title(datum_df)
        if datum_options is not None:
            if "fill" in datum_options:
                if datum_options["fill"][0] == "linear":
                    try:
                        datum_df[datum_df_columns] = datum_df[datum_df_columns].interpolate(method="time")
                    except TypeError as type_error:
                        anode.Log(logging.WARN).log("Plugin", "state",
                                                    lambda: "[{}] could not interperloate data frame column [{}]".format(self.name, type_error))
                if datum_options["fill"][0] == "linear" or datum_options["fill"][0] == "forwardback":
                    datum_df[datum_df_columns] = datum_df[datum_df_columns].fillna(method="ffill").fillna(method="bfill")
                if datum_options["fill"][0] == "linear" or datum_options["fill"][0] == "zeros":
                    datum_df[datum_df_columns] = datum_df[datum_df_columns].fillna(0)
        Plugin.datums_df_title(datum_df, datum_df_title)
        return datum_df

    @staticmethod
    def datums_df_data_pretty(datum_df):
        datum_df_title = Plugin.datums_df_title(datum_df)
        timestamps = []
        if "Time" in datum_df:
            timestamps.append("Time")
        if "bin_timestamp" in datum_df:
            timestamps.append("bin_timestamp")
        for timestamp in timestamps:
            datum_df[timestamp] = datum_df[timestamp].apply(
                lambda epoch: datetime.datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S"))
        Plugin.datums_df_title(datum_df, datum_df_title)
        return datum_df

    @staticmethod
    def datums_df_metadata_pretty(datum_df):
        datum_df_columns_deletes = []
        datum_df_columns_renames = {}
        datum_df_columns_renames_order = {}
        datum_df_columns_renames_tokens = {}
        datum_df_columns_renames_tokens_unit = set()
        datum_df_columns_renames_tokens_metric0 = set()
        datum_df_columns_renames_tokens_metric2 = set()
        datum_df_columns_renames_tokens_metric2_type = set()
        datum_df_columns_renames_tokens_metric3_type = set()
        for datum_df_column in datum_df.columns:
            if datum_df_column == "bin_timestamp":
                datum_df_columns_renames[datum_df_column] = "Time"
                datum_df_columns_renames_order[datum_df_column] = "0"
            elif datum_df_column.startswith("data_value"):
                datum_df_column_tokens_all = datum_df_column.split("data_value_scaled@")[1].split("&")
                datum_df_column_tokens_metric = datum_df_column_tokens_all[0].split("=")[1]
                datum_df_column_tokens_subset = datum_df_column_tokens_metric.split(".")
                datum_df_column_tokens_subset.append(datum_df_column_tokens_all[1].split("=")[1])
                datum_df_columns_renames_tokens_unit.add(
                    urllib.unquote_plus(datum_df_column_tokens_all[3].split("=")[1].encode("utf-8")).decode("utf-8"))
                datum_df_columns_renames_tokens_metric0.add(datum_df_column_tokens_subset[0])
                datum_df_columns_renames_tokens_metric2.add(datum_df_column_tokens_subset[2])
                datum_df_columns_renames_tokens_metric2_type.add(
                    "".join([datum_df_column_tokens_subset[2], datum_df_column_tokens_subset[3]]))
                datum_df_columns_renames_tokens_metric3_type.add("".join(datum_df_column_tokens_subset[0:4]))
                datum_df_column_tokens_subset.append("".join(["(", " ".join(
                    [re.search(r"\d+", datum_df_column_tokens_all[2].split("=")[1]).group(), datum_df_column_tokens_all[2].split("=")[1].replace(
                        re.search(r"\d+", datum_df_column_tokens_all[2].split("=")[1]).group(), "").title()]), ")"]))
                datum_df_columns_renames_order[datum_df_column] = \
                    (str(DATUM_SCHEMA_METRICS[datum_df_column_tokens_metric]) + datum_df_column_tokens_all[2]) \
                        if datum_df_column_tokens_all[0].split("=")[1] in DATUM_SCHEMA_METRICS else datum_df_column_tokens_metric
                datum_df_columns_renames_tokens[datum_df_column] = datum_df_column_tokens_subset
            elif datum_df_column.startswith("data_timestamp"):
                datum_df_columns_deletes.append(datum_df_column)
        for datum_df_columns_delete in datum_df_columns_deletes:
            del datum_df[datum_df_columns_delete]
        for datum_df_columns_renames_token in datum_df_columns_renames_tokens:
            datum_df_columns_renames_token_subset = []
            if len(datum_df_columns_renames_tokens_metric0) > 1:
                datum_df_columns_renames_token_subset.append(datum_df_columns_renames_tokens[datum_df_columns_renames_token][0].title())
            if len(datum_df_columns_renames_tokens_metric2) == len(datum_df_columns_renames_tokens):
                datum_df_columns_renames_token_subset.append(datum_df_columns_renames_tokens[datum_df_columns_renames_token][2].title())
            else:
                datum_df_columns_renames_tokens_metric2_type_str = datum_df_columns_renames_tokens[datum_df_columns_renames_token][2].title() if \
                    (datum_df_columns_renames_tokens[datum_df_columns_renames_token][3] == "point" or
                     datum_df_columns_renames_tokens[datum_df_columns_renames_token][3] == "integral") else " ".join(
                    [datum_df_columns_renames_tokens[datum_df_columns_renames_token][2].title(),
                     datum_df_columns_renames_tokens[datum_df_columns_renames_token][3].title()])
                if len(datum_df_columns_renames_tokens_metric2_type) == len(datum_df_columns_renames_tokens):
                    datum_df_columns_renames_token_subset.append(datum_df_columns_renames_tokens_metric2_type_str)
                elif len(datum_df_columns_renames_tokens_metric3_type) == len(datum_df_columns_renames_tokens):
                    datum_df_columns_renames_token_subset.append(
                        " ".join([datum_df_columns_renames_tokens[datum_df_columns_renames_token][1].title(),
                                  datum_df_columns_renames_tokens_metric2_type_str]))
                else:
                    datum_df_columns_renames_token_subset.append(
                        " ".join([datum_df_columns_renames_tokens[datum_df_columns_renames_token][1].title(),
                                  datum_df_columns_renames_tokens_metric2_type_str,
                                  datum_df_columns_renames_tokens[datum_df_columns_renames_token][4]]))
            datum_df_columns_renames[datum_df_columns_renames_token] = " ".join(datum_df_columns_renames_token_subset)
        if len(datum_df_columns_renames) == 2:
            for datum_df_column_old, datum_df_column_new in datum_df_columns_renames.iteritems():
                if datum_df_column_old.startswith("data_value_scaled@"):
                    datum_df_columns_renames[datum_df_column_old] = \
                        " ".join([datum_df_column_new, datum_df_columns_renames_tokens[datum_df_column_old][4]])
        for datum_df_columns_rename in datum_df_columns_renames:
            datum_df_columns_renames[datum_df_columns_rename] = datum_df_columns_renames[datum_df_columns_rename]. \
                replace("-", " ").replace("1 All Time", "All Time")
        datum_df.rename(columns=datum_df_columns_renames, inplace=True)
        datum_df_columns_reorder = []
        for datum_df_column_old in sorted(datum_df_columns_renames_order,
                                          key=lambda datum_df_column_sort: (datum_df_columns_renames_order[datum_df_column_sort])):
            datum_df_columns_reorder.append(datum_df_columns_renames[datum_df_column_old])
        datum_df = datum_df[datum_df_columns_reorder]
        Plugin.datums_df_title(datum_df, (" & ".join(datum_df_columns_renames_tokens_metric0).title() +
                                          " (" + ", ".join(datum_df_columns_renames_tokens_unit) + ")").replace("-", " "))
        return datum_df

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

    def datums_store(self):
        log_timer = anode.Log(logging.INFO).start()
        for datum_metric in self.datums:
            for datum_type in self.datums[datum_metric]:
                for datum_unit in self.datums[datum_metric][datum_type]:
                    for datum_bin in self.datums[datum_metric][datum_type][datum_unit]:
                        if DATUM_QUEUE_HISTORY in self.datums[datum_metric][datum_type][datum_unit][datum_bin]:
                            self.datum_merge_buffer_history(self.datums[datum_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_BUFFER],
                                                            self.datums[datum_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_HISTORY])
                        else:
                            self.datums[datum_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_BUFFER].clear()
        if len(self.datums) > 0:
            pandas.to_pickle(self.datums, self.datums_file())
        metrics_count = sum(len(units)
                            for metrics in self.datums.values()
                            for types in metrics.values()
                            for units in types.values())
        datums_count = sum(0 if DATUM_QUEUE_HISTORY not in bins else (
            sum(len(partitions["data_df"].index) for partitions in bins[DATUM_QUEUE_HISTORY].values()))
                           for metrics in self.datums.values()
                           for types in metrics.values()
                           for units in types.values()
                           for bins in units.values())
        log_timer.log("Plugin", "timer", lambda: "[{}] state stored [{}] metrics and [{}] datums".format(self.name, metrics_count, datums_count),
                      context=self.datums_store)

    def datums_load(self):
        log_timer = anode.Log(logging.INFO).start()
        metrics_count = 0
        if os.path.isfile(self.datums_file()):
            self.datums = pandas.read_pickle(self.datums_file())
            metrics_count = sum(len(units)
                                for metrics in self.datums.values()
                                for types in metrics.values()
                                for units in types.values())
        for datum_metric in self.datums:
            for datum_type in self.datums[datum_metric]:
                for datum_unit in self.datums[datum_metric][datum_type]:
                    for datum_bin in self.datums[datum_metric][datum_type][datum_unit]:
                        self.datums[datum_metric][datum_type][datum_unit][datum_bin][DATUM_QUEUE_BUFFER].clear()
        datums_count = sum(0 if DATUM_QUEUE_HISTORY not in bins else (
            sum(len(partitions["data_df"].index) for partitions in bins[DATUM_QUEUE_HISTORY].values()))
                           for metrics in self.datums.values()
                           for types in metrics.values()
                           for units in types.values()
                           for bins in units.values())
        log_timer.log("Plugin", "timer", lambda: "[{}] state loaded [{}] metrics and [{}] datums".format(self.name, metrics_count, datums_count),
                      context=self.datums_store)

    def datums_file(self):
        return self.config["db_dir"] + "/" + self.name + ".pkl"

    def get_time(self):
        return calendar.timegm(time.gmtime()) if not self.is_clock else ((1 if self.reactor.seconds() == 0 else int(self.reactor.seconds())) +
                                                                         self.get_time_period(self.time_boot, 24 * 60 * 60))

    def get_time_period(self, timestamp, period):
        return period if period > timestamp else \
            ((timestamp + self.time_tmz_offset) - (timestamp + self.time_tmz_offset) % period - self.time_tmz_offset)

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
        elif unit == "day-time":
            return scalor * 60 * 60 * 24
        elif unit == "night-time":
            return scalor * 60 * 60 * 24
        elif unit == "month":
            return scalor * 60 * 60 * 24 * 30.42
        elif unit == "year":
            return scalor * 60 * 60 * 24 * 365
        elif unit == "all-time":
            return scalor * -1
        else:
            raise Exception("Unknown time unit [{}]".format(unit))

    @staticmethod
    def get(parent, plugin_name, config, reactor):
        plugin = getattr(import_module("anode.plugin") if hasattr(anode.plugin, plugin_name.title()) else
                         import_module("anode.plugin." + plugin_name), plugin_name.title())(parent, plugin_name, config, reactor)
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] initialised".format(plugin_name))
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
        self.time_seen = None
        self.time_boot = calendar.timegm(time.gmtime())
        time_local = time.localtime()
        self.time_tmz_offset = calendar.timegm(time_local) - calendar.timegm(time.gmtime(time.mktime(time_local)))
        self.datums_buffer_batch = BUFFER_BATCH_DEFAULT if ("buffer_ticks" not in self.config or self.config["buffer_ticks"] < 1) \
            else self.config["buffer_ticks"]
        self.datums_load()


BUFFER_BATCH_DEFAULT = 60

SERIALISATION_BATCH = 1000
SERIALISATION_BATCH_SLEEP = 0.3

DATUM_TIMESTAMP_MIN = -2211753600
DATUM_TIMESTAMP_MAX = 32500915200

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
DATUM_SCHEMA_MODEL = {DATUM_SCHEMA_JSON[7]["fields"][i]["name"]: i * 10 for i in range(len(DATUM_SCHEMA_JSON[7]["fields"]))}
DATUM_SCHEMA_METRICS = {DATUM_SCHEMA_JSON[2]["symbols"][i]: i * 10 for i in range(len(DATUM_SCHEMA_JSON[2]["symbols"]))}

ID_BYTE = format(get_mac(), "x").decode("hex")
ID_HEX = ID_BYTE.encode("hex")
ID_BASE64 = base64.b64encode(str(ID_BYTE))

SVG_EMPTY = """<?xml version="1.0" encoding="utf-8" standalone="no"?>
<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
  "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">
<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
</svg>"""
