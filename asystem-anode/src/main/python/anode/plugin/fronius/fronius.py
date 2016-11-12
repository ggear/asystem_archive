# -*- coding: utf-8 -*-

from __future__ import print_function

import calendar
import json
import logging
import time
from decimal import Decimal

import dateutil.parser
import treq
from anode.plugin.plugin import Plugin

HTTP_TIMEOUT = 5


class Fronius(Plugin):
    def poll(self):
        self.http_get("http://10.0.1.203/solar_api/v1/GetPowerFlowRealtimeData.fcgi", self.push_flow)
        self.http_get("http://10.0.1.203/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System", self.push_meter)

    # noinspection PyShadowingNames
    def http_get(self, url, callback):
        connection_pool = self.config["pool"] if "poll" in self.config else None
        treq.get(url, timeout=HTTP_TIMEOUT, pool=connection_pool).addCallbacks(
            lambda response, url=url, callback=callback: self.http_response(response, url, callback),
            errback=lambda error, url=url: logging.getLogger().error(
                "Error processing HTTP GET [{}] with [{}]".format(url, error.getErrorMessage()))
            if logging.getLogger().isEnabledFor(logging.ERROR) else None)

    @staticmethod
    def http_response(response, url, callback):
        if response.code == 200:
            treq.text_content(response).addCallbacks(callback)
        else:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.getLogger().error("Error processing HTTP GET [{}] with [{}]".format(url, response.code))

    def push_flow(self, text_content):
        json_content = json.loads(text_content, parse_float=Decimal)
        bin_timestamp = calendar.timegm(time.gmtime())
        data_timestamp = int(time.mktime(dateutil.parser.parse(json_content["Head"]["Timestamp"]).timetuple()))
        self.datum_push(
            "power.production.grid",
            "point",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Grid"], -100) if self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Grid"]) < 0 else 0,
            "W",
            100,
            data_timestamp,
            bin_timestamp,
            self.config["poll"],
            "s"
        )
        self.datum_push(
            "power.production.battery",
            "point",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Akku"], -100) if self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Akku"]) < 0 else 0,
            "W",
            100,
            data_timestamp,
            bin_timestamp,
            self.config["poll"],
            "s"
        )
        self.datum_push(
            "power.production.inverter",
            "point",
            self.get_int_scaled(json_content["Body"]["Data"]["Inverters"]["1"]["P"], 100),
            "W",
            100,
            data_timestamp,
            bin_timestamp,
            self.config["poll"],
            "s"
        )
        self.datum_push(
            "power.production.array",
            "point",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_PV"], 100),
            "W",
            100,
            data_timestamp,
            bin_timestamp,
            self.config["poll"],
            "s"
        )
        self.datum_push(
            "power.consumption.grid",
            "point",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Grid"], 100) if self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Grid"]) > 0 else 0,
            "W",
            100,
            data_timestamp,
            bin_timestamp,
            self.config["poll"],
            "s"
        )
        self.datum_push(
            "power.consumption.battery",
            "point",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Akku"], 100) if self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Akku"]) > 0 else 0,
            "W",
            100,
            data_timestamp,
            bin_timestamp,
            self.config["poll"],
            "s"
        )
        self.datum_push(
            "power.consumption.inverter",
            "point",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Load"], -100) -
            (self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Grid"], 100) if self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Grid"]) > 0 else 0) -
            (self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Akku"], 100) if self.get_int_scaled(json_content["Body"]["Data"]["Site"]["P_Akku"]) > 0 else 0),
            "W",
            100,
            data_timestamp,
            bin_timestamp,
            self.config["poll"],
            "s"
        )
        self.datum_push(
            "energy.production.array",
            "integral",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["E_Day"], 10),
            "Wh",
            10,
            data_timestamp,
            bin_timestamp,
            1,
            "d"
        )
        self.datum_push(
            "energy.production.array",
            "integral",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["E_Year"], 10),
            "Wh",
            10,
            data_timestamp,
            bin_timestamp,
            1,
            "y"
        )
        self.datum_push(
            "energy.production.array",
            "integral",
            self.get_int_scaled(json_content["Body"]["Data"]["Site"]["E_Total"], 10),
            "Wh",
            10,
            data_timestamp,
            bin_timestamp,
            1,
            u"∀"
        )
        self.datum_pop()

    def push_meter(self, text_content):
        json_content = json.loads(text_content, parse_float=Decimal)
        bin_timestamp = calendar.timegm(time.gmtime())
        data_timestamp = int(time.mktime(dateutil.parser.parse(json_content["Head"]["Timestamp"]).timetuple()))
        self.datum_push(
            "energy.production.grid",
            "integral",
            self.get_int_scaled(json_content["Body"]["Data"]["0"]["EnergyReal_WAC_Minus_Absolute"], 10),
            "Wh",
            10,
            data_timestamp,
            bin_timestamp,
            1,
            u"∀"
        )
        self.datum_push(
            "energy.consumption.grid",
            "integral",
            self.get_int_scaled(json_content["Body"]["Data"]["0"]["EnergyReal_WAC_Plus_Absolute"], 10),
            "Wh",
            10,
            data_timestamp,
            bin_timestamp,
            1,
            u"∀"
        )
        self.datum_pop()
