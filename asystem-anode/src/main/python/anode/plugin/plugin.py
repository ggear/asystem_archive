from __future__ import print_function

import HTMLParser
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
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] dropped [{}] datums".format(self.name, self.datums_dropped))
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] pushed datums".format(self.name, self.datums_pushed))
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] saved datums".format(self.name, self.datums_history))
        if "push_upstream" in self.config and self.config["push_upstream"]:
            for datum_metric in self.datums:
                for datum_type in self.datums[datum_metric]:
                    for datum_bin in self.datums[datum_metric][datum_type]:
                        # noinspection PyCompatibility
                        for i in xrange(len(self.datums[datum_metric][datum_type][datum_bin][DATUM_QUEUE_PUBLISH])):
                            datum_avro = self.datums[datum_metric][datum_type][datum_bin][DATUM_QUEUE_PUBLISH].popleft()
                            anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] popped datum [{}]".format(
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
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] popped [{}] datums".format(self.name, datums_popped))

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
                anode.Log(logging.DEBUG).log("Plugin", "state",
                                             lambda: "[{}] upperbounded datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
            if data_bound_lower is not None and data_value < Decimal(data_bound_lower * data_scale):
                datum_dict["data_value"] = data_bound_lower * data_scale
                anode.Log(logging.DEBUG).log("Plugin", "state",
                                             lambda: "[{}] lowerbounded datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
            try:
                datum_avro = self.datum_dict_to_avro(datum_dict)
            except AvroTypeException as exception:
                anode.Log(logging.ERROR).log("Plugin", "error",
                                             lambda: "[{}] error serialising Avro object [{}]".format(self.name, exception), exception)
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
                            anode.Log(logging.DEBUG).log("Plugin", "state",
                                                         lambda: "[{}] seleted high [{}]".format(self.name,
                                                                                                 self.datum_tostring(datums_deref[DATUM_QUEUE_MAX])))
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
                                    "history_seconds" in self.config and self.config["history_seconds"] > 0:
                        datum_history_peek = None
                        if len(datums_deref[DATUM_QUEUE_HISTORY]) > 0:
                            datum_history_peek = datums_deref[DATUM_QUEUE_HISTORY].popleft()
                        if datum_history_peek is not None:
                            if self.config["history_ticks"] <= self.datums_history or \
                                    ((calendar.timegm(time.gmtime()) - self.datum_avro_to_dict(datum_history_peek)[
                                        "bin_timestamp"]) >= self.config["history_seconds"]):
                                anode.Log(logging.DEBUG).log("Plugin", "state",
                                                             lambda: "[{}] seleted datum [{}]".format(self.name, self.datum_tostring(
                                                                 self.datum_avro_to_dict(datum_history_peek))))
                            else:
                                datums_deref[DATUM_QUEUE_HISTORY].appendleft(datum_history_peek)
                                self.datums_history += 1
                        datums_deref[DATUM_QUEUE_HISTORY].append(datum_avro)
                        anode.Log(logging.DEBUG).log("Plugin", "state",
                                                     lambda: "[{}] saved datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
                anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] pushed datum [{}]".format(self.name, self.datum_tostring(datum_dict)))
                self.anode.push_datums({"dict": [datum_dict]})
            else:
                self.datums_dropped += 1
                anode.Log(logging.DEBUG).log("Plugin", "state", lambda: "[{}] dropped datum [{}]".format(self.name, self.datum_tostring(datum_dict)))

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

    def datums_filter_get(self, datums_filtered, datum_filter):
        log_timer = anode.Log(logging.DEBUG).start()
        for data_metric in self.datums:
            if Plugin.is_fitlered(datum_filter, "metrics", data_metric):
                for datum_type in self.datums[data_metric]:
                    if Plugin.is_fitlered(datum_filter, "types", datum_type):
                        for datum_bin in self.datums[data_metric][datum_type]:
                            if Plugin.is_fitlered(datum_filter, "bins", datum_bin):
                                datum_scopes = [DATUM_QUEUE_LAST] if "scope" not in datum_filter else datum_filter["scope"]
                                for datum_scope in datum_scopes:
                                    if datum_scope in self.datums[data_metric][datum_type][datum_bin]:
                                        datums = [self.datums[data_metric][datum_type][datum_bin][datum_scope]] \
                                            if datum_scope == DATUM_QUEUE_LAST else self.datums[data_metric][datum_type][datum_bin][datum_scope]
                                        datums_format = "dict" if datum_scope == DATUM_QUEUE_LAST else "avro"
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
        return avro_writer.getvalue()

    @staticmethod
    def datum_dict_to_json(datum_dict):
        datum_dict = datum_dict.copy()
        if "anode_id" in datum_dict:
            datum_dict["anode_id"] = ID_HEX
        return json.dumps(datum_dict, separators=(',', ':'))

    @staticmethod
    def datum_dict_to_csv(datum_dict):
        datum_dict = datum_dict.copy()
        if "anode_id" in datum_dict:
            datum_dict["anode_id"] = ID_BASE64
            if datum_dict["data_unit"] not in DATUM_SCHEMA_TO_HTML:
                DATUM_SCHEMA_TO_HTML[datum_dict["data_unit"]] = EntitySubstitution().substitute_html(datum_dict["data_unit"])
            datum_dict["data_unit"] = DATUM_SCHEMA_TO_HTML[datum_dict["data_unit"]]
            if datum_dict["bin_unit"] not in DATUM_SCHEMA_TO_HTML:
                DATUM_SCHEMA_TO_HTML[datum_dict["bin_unit"]] = EntitySubstitution().substitute_html(datum_dict["bin_unit"])
            datum_dict["bin_unit"] = DATUM_SCHEMA_TO_HTML[datum_dict["bin_unit"]]
        return ','.join(str(datum_dict[datum_field["name"]]) for datum_field in DATUM_SCHEMA_JSON[7]["fields"])

    @staticmethod
    def datum_avro_to_dict(datum_avro):
        return avro.io.DatumReader(DATUM_SCHEMA_AVRO).read(avro.io.BinaryDecoder(io.BytesIO(datum_avro)))

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
                elif datum_format != "dict":
                    raise ValueError("Unkown datum format conversion [{}] to [{}]".format(datums_format, datum_format))
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
                    datums_formatted["dict" if datum_to_format_iterate else datum_format].append(datums_format_function(datum))
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
    def datums_dict_to_json(datums_dict, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        count = 0
        datums_json_fragments = []
        for datum_dict in datums_dict:
            datums_json_fragments.append(Plugin.datum_dict_to_json(datum_dict))
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
    def datums_dict_to_csv(datums_dict, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        count = 0
        datums_csv_fragments = [','.join(str(datum_field["name"]) for datum_field in DATUM_SCHEMA_JSON[7]["fields"]) if len(datums_dict) > 0 else ""]
        for datum_dict in datums_dict:
            datums_csv_fragments.append(Plugin.datum_dict_to_csv(datum_dict))
            count += 1
            if count % SERIALISATION_BATCH == 0 or count == len(datums_dict):
                datums_csv_fragments = ["\n".join(datums_csv_fragments)]
                if off_thread and count < len(datums_dict):
                    log_timer.pause()
                    time.sleep(SERIALISATION_BATCH_SLEEP)
                    log_timer.start()
        datums_csv = "".join([datums_csv_fragments[0], "\n"])
        log_timer.log("Plugin", "timer", lambda: "[*]", context=Plugin.datums_dict_to_csv, off_thread=off_thread)
        return datums_csv

    @staticmethod
    def datums_csv_to_dict(datums_csv):
        datums_dict = {"dict": []}
        for datum_dict in datums_csv:
            datum_dict["anode_id"] = base64.b64decode(datum_dict["anode_id"])
            datum_dict["data_value"] = long(datum_dict["data_value"])
            datum_dict["data_unit"] = HTMLParser.HTMLParser().unescape(datum_dict["data_unit"])
            if datum_dict["data_unit"] not in DATUM_SCHEMA_FROM_HTML:
                DATUM_SCHEMA_FROM_HTML[datum_dict["data_unit"]] = HTMLParser.HTMLParser().unescape(datum_dict["data_unit"])
            datum_dict["data_scale"] = float(datum_dict["data_scale"])
            datum_dict["data_timestamp"] = long(datum_dict["data_timestamp"])
            datum_dict["bin_timestamp"] = long(datum_dict["bin_timestamp"])
            datum_dict["bin_width"] = int(datum_dict["bin_width"])
            if datum_dict["bin_unit"] not in DATUM_SCHEMA_FROM_HTML:
                DATUM_SCHEMA_FROM_HTML[datum_dict["bin_unit"]] = HTMLParser.HTMLParser().unescape(datum_dict["bin_unit"])
            datums_dict["dict"].append(datum_dict)
        return datums_dict

    @staticmethod
    def datums_to_format(datums, datum_format, off_thread=False):
        log_timer = anode.Log(logging.DEBUG).start()
        datums_formatted = Plugin.datum_to_format(datums, "dict", off_thread)["dict"]
        if datum_format == "json":
            datums_formatted = Plugin.datums_dict_to_json(datums_formatted, off_thread)
        elif datum_format == "csv":
            datums_formatted = Plugin.datums_dict_to_csv(datums_formatted, off_thread)
        elif datum_format != "dict":
            raise ValueError("Unkown datum format [{}]".format(datum_format))
        log_timer.log("Plugin", "timer", lambda: "[*] {} to {} for [{}] datums".format(",".join(datums.keys()), datum_format, sum(
            len(datums_values) for datums_values in datums.values())), context=Plugin.datums_to_format, off_thread=off_thread)
        return datums_formatted

    def datum_value(self, data, keys=None, default=None, factor=1):
        # noinspection PyBroadException
        try:
            value = data if keys is None else reduce(operator.getitem, keys, data)
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
        anode.Log(logging.INFO).log("Plugin", "state", lambda: "[{}] initialised".format(module))
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


SERIALISATION_BATCH = 1000
SERIALISATION_BATCH_SLEEP = 0.3

DATUM_QUEUE_MIN = "min"
DATUM_QUEUE_MAX = "max"
DATUM_QUEUE_LAST = "last"
DATUM_QUEUE_PUBLISH = "publish"
DATUM_QUEUE_HISTORY = "history"

DATUM_SCHEMA_TO_HTML = {}
DATUM_SCHEMA_FROM_HTML = {}
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
