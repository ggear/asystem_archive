from __future__ import print_function

import abc
import base64
import io
import json
import logging
import os
from collections import deque
from decimal import Decimal
from importlib import import_module
from uuid import getnode as get_mac

import anode.plugin
import avro
import avro.io
import avro.schema
import avro.schema
from avro.io import AvroTypeException


class Plugin(object):
    # noinspection PyStatementEffect
    @abc.abstractmethod
    def poll(self):
        None

    def datum_ping(self):
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] pinged [{}] datums".format(self.name, 0))

    def datum_pop(self):
        datums_popped = 0
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] dropped [{}] datums".format(self.name, self.datums_dropped))
            logging.getLogger().info("Plugin [{}] pushed [{}] datums".format(self.name, self.datums_pushed))
        if "push" in self.config and self.config["push"]:
            for datum_metric in self.datums:
                for datum_type in self.datums[datum_metric]:
                    for datum_bin in self.datums[datum_metric][datum_type]:
                        # noinspection PyCompatibility
                        for i in xrange(len(self.datums[datum_metric][datum_type][datum_bin]["queue"])):
                            datum_avro = self.datums[datum_metric][datum_type][datum_bin]["queue"].popleft()
                            if logging.getLogger().isEnabledFor(logging.DEBUG):
                                logging.getLogger().debug("Popped datum [{}]".format(self.datum_tostring(self.datum_avro_to_dict(datum_avro))))
                            # TDOD: push to QMTT broker, returning datums to left of deque if push fails
                            datums_popped += 1
        self.datums_pushed = 0
        self.datums_dropped = 0
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] popped [{}] datums".format(self.name, datums_popped))

    def datum_push(self, data_metric, data_type, data_value, data_unit, data_scale, data_timestamp, bin_timestamp, bin_width, bin_unit):
        datum_dict = {
            "anode_id": self.mac_address,
            "data_source": self.name,
            "data_metric": data_metric,
            "data_type": data_type,
            "data_value": data_value,
            "data_unit": data_unit,
            "data_scale": data_scale,
            "data_timestamp": data_timestamp,
            "bin_timestamp": bin_timestamp,
            "bin_width": bin_width,
            "bin_unit": bin_unit
        }
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
            self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][str(datum_dict["bin_width"]) + datum_dict["bin_unit"]] = {"queue": deque()}
        datums_deref = self.datums[datum_dict["data_metric"]][datum_dict["data_type"]][str(datum_dict["bin_width"]) + datum_dict["bin_unit"]]
        if "last" not in datums_deref or datums_deref["last"]["data_value"] != datum_dict["data_value"] or datums_deref["last"]["data_unit"] != \
                datum_dict["data_unit"] or datums_deref["last"]["data_scale"] != datum_dict["data_scale"]:
            datums_deref["last"] = datum_dict
            datums_deref["queue"].append(datum_avro)
            self.datums_pushed += 1
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.getLogger().debug("Pushed datum [{}]".format(self.datum_tostring(datum_dict)))
        else:
            self.datums_dropped += 1
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.getLogger().debug("Dropped datum [{}]".format(self.datum_tostring(datum_dict)))

    def datum_tostring(self, datum_dict):
        return "{}.{}={}{}".format(
            datum_dict["data_source"],
            datum_dict["data_metric"],
            int(self.datum_avro_to_dict(self.datum_dict_to_avro(datum_dict))["data_value"]) / Decimal(datum_dict["data_scale"]),
            datum_dict["data_unit"]
        )

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
        if "anode_id" in datum_dict:
            datum_dict["anode_id"] = base64.b64encode(datum_dict["anode_id"])
        return json.dumps(datum_dict, separators=(',', ':'))

    @staticmethod
    def get_int_scaled(numeric, factor=1):
        return 0 if numeric is None else int(numeric * factor)

    @staticmethod
    def get(module, config):
        plugin = getattr(import_module("anode.plugin") if hasattr(anode.plugin, module.title()) else import_module("anode.plugin." + module), module.title())(module, config)
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [{}] initialised".format(module))
        return plugin

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.mac_address = format(get_mac(), "x").decode("hex")
        self.datums = {}
        self.datums_pushed = 0
        self.datums_dropped = 0


DATUM_SCHEMA = avro.schema.parse(open(os.path.dirname(__file__) + "/../model/datum.avsc", "rb").read())


class Publish(Plugin):
    # noinspection PyStatementEffect
    def poll(self):
        None


class Callback(Plugin):
    def poll(self):
        if "callback" in self.config and self.config["callback"] is not None:
            self.config["callback"]()
