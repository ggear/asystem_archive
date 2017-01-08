# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function

import calendar
import json
import logging
import time
from decimal import Decimal

import dateutil.parser
import treq

from anode.plugin.plugin import DATUM_QUEUE_LAST
from anode.plugin.plugin import DATUM_QUEUE_MIN
from anode.plugin.plugin import Plugin

HTTP_TIMEOUT = 5
POLL_METER_ITERATIONS = 5


# noinspection PyBroadException
class Fronius(Plugin):
    def _poll(self):
        self.http_get("http://10.0.1.203/solar_api/v1/GetPowerFlowRealtimeData.fcgi", self.push_flow)
        if self.poll_meter_iteration == POLL_METER_ITERATIONS:
            self.poll_meter_iteration = 0
            self.http_get("http://10.0.1.203/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System", self.push_meter)
        else:
            self.poll_meter_iteration += 1

    # noinspection PyShadowingNames
    def http_get(self, url, callback):
        connection_pool = self.config["pool"] if "pool" in self.config else None
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
                logging.getLogger().error("Error processing HTTP response [{}] with [{}]".format(url, response.code))

    def push_flow(self, text_content):
        if logging.getLogger().isEnabledFor(logging.INFO):
            time_start = time.time()
        try:
            dict_content = json.loads(text_content, parse_float=Decimal)
            bin_timestamp = calendar.timegm(time.gmtime())
            data_timestamp = int(calendar.timegm(dateutil.parser.parse(dict_content["Head"]["Timestamp"]).timetuple()))
            self.datum_push(
                "power.production.grid",
                "current", "point",
                self.datum_value(dict_content, ["Body", "Data", "Site", "P_Grid"], 0, -1) if self.datum_value(
                    dict_content, ["Body", "Data", "Site", "P_Grid"], 0) <= 0 else 0,
                "W",
                1,
                data_timestamp,
                bin_timestamp,
                self.config["poll_seconds"],
                "second",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            # TODO: Do not include until batteries installed
            # self.datum_push(
            #     "power.production.battery",
            #     "current", "point",
            #     self.datum_value(dict_content, ["Body", "Data", "Site", "P_Akku"], 0, -1) if self.datum_value(
            #         dict_content, ["Body", "Data", "Site", "P_Akku"], 0) <= 0 else 0,
            #     "W",
            #     1,
            #     data_timestamp,
            #     bin_timestamp,
            #     self.config["poll_seconds"],
            #     "second",
            #     data_bound_lower=0,
            #     data_derived_max=True,
            #     data_derived_min=True
            # )
            self.datum_push(
                "power.production.inverter",
                "current", "point",
                self.datum_value(dict_content, ["Body", "Data", "Inverters", "1", "P"], 0, 1),
                "W",
                1,
                data_timestamp,
                bin_timestamp,
                self.config["poll_seconds"],
                "second",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_push(
                "power.consumption.grid",
                "current", "point",
                self.datum_value(dict_content, ["Body", "Data", "Site", "P_Grid"], 0, 1) if self.datum_value(
                    dict_content, ["Body", "Data", "Site", "P_Grid"], 0) >= 0 else 0,
                "W",
                1,
                data_timestamp,
                bin_timestamp,
                self.config["poll_seconds"],
                "second",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            # TODO: Do not include until batteries installed
            # self.datum_push(
            #     "power.consumption.battery",
            #     "current", "point",
            #     self.datum_value(dict_content, ["Body", "Data", "Site", "P_Akku"], 0, 1) if self.datum_value(
            #         dict_content, ["Body", "Data", "Site", "P_Akku"], 0) >= 0 else 0,
            #     "W",
            #     1,
            #     data_timestamp,
            #     bin_timestamp,
            #     self.config["poll_seconds"],
            #     "second",
            #     data_bound_lower=0,
            #     data_derived_max=True,
            #     data_derived_min=True
            # )
            # self.datum_push(
            #     "power.consumption.inverter",
            #     "current", "point",
            #     self.datum_value(dict_content, ["Body", "Data", "Site", "P_Load"], 0, -1) -
            #     (self.datum_value(dict_content, ["Body", "Data", "Site", "P_Grid"], 0, 1) if self.datum_value(
            #         dict_content, ["Body", "Data", "Site", "P_Grid"], 0) >= 0 else 0) -
            #     (self.datum_value(dict_content, ["Body", "Data", "Site", "P_Akku"], 0, 1) if self.datum_value(
            #         dict_content, ["Body", "Data", "Site", "P_Akku"], 0) >= 0 else 0),
            #     "W",
            #     1,
            #     data_timestamp,
            #     bin_timestamp,
            #     self.config["poll_seconds"],
            #     "second",
            #     data_bound_lower=0,
            #     data_derived_max=True,
            #     data_derived_min=True
            # )
            self.datum_push(
                "power.consumption.inverter",
                "current", "point",
                self.datum_value(dict_content, ["Body", "Data", "Site", "P_Load"], 0, -1) -
                (self.datum_value(dict_content, ["Body", "Data", "Site", "P_Grid"], 0, 1) if self.datum_value(
                    dict_content, ["Body", "Data", "Site", "P_Grid"], 0) >= 0 else 0),
                "W",
                1,
                data_timestamp,
                bin_timestamp,
                self.config["poll_seconds"],
                "second",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_push(
                "power.utlisation.inverter",
                "current", "point",
                self.datum_value(dict_content, ["Body", "Data", "Site", "rel_SelfConsumption"], 0),
                "%",
                1,
                data_timestamp,
                bin_timestamp,
                self.config["poll_seconds"],
                "second",
                data_bound_upper=100,
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            # TODO: Do not include until batteries installed
            # self.datum_push(
            #     "power.utlisation.battery",
            #     "current", "point",
            #     self.datum_value(0),
            #     "%",
            #     1,
            #     data_timestamp,
            #     bin_timestamp,
            #     self.config["poll_seconds"],
            #     "second",
            #     data_bound_upper=100,
            #     data_bound_lower=0,
            #     data_derived_max=True,
            #     data_derived_min=True
            # )
            self.datum_push(
                "power.utlisation.grid",
                "current", "point",
                self.datum_value(100 - self.datum_value(dict_content, ["Body", "Data", "Site", "rel_Autonomy"], 0)),
                "%",
                1,
                data_timestamp,
                bin_timestamp,
                self.config["poll_seconds"],
                "second",
                data_bound_upper=100,
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_push(
                "power.utlisation.array",
                "current", "point",
                0 if self.datum_value(dict_content, ["Body", "Data", "Site", "P_PV"], 0) == 0 else (
                    self.datum_value(self.datum_value(dict_content, ["Body", "Data", "Inverters", "1", "P"], 0) / self.datum_value(
                        dict_content, ["Body", "Data", "Site", "P_PV"], 0) * 100)),
                "%",
                1,
                data_timestamp,
                bin_timestamp,
                self.config["poll_seconds"],
                "second",
                data_bound_upper=100,
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_push(
                "energy.production.inverter",
                "current", "integral",
                self.datum_value(dict_content, ["Body", "Data", "Site", "E_Day"], factor=10),
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_push(
                "energy.production.inverter",
                "current", "integral",
                self.datum_value(dict_content, ["Body", "Data", "Site", "E_Year"], factor=10),
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "year",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_push(
                "energy.production.inverter",
                "current", "integral",
                self.datum_value(dict_content, ["Body", "Data", "Site", "E_Total"], factor=10),
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "alltime",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_pop()
        except Exception:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.exception(
                    "Unexpected error processing response [{}]".format(text_content))
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info(
                "Plugin [{}] push_flow on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))

    def push_meter(self, text_content):
        if logging.getLogger().isEnabledFor(logging.INFO):
            time_start = time.time()
        try:
            dict_content = json.loads(text_content, parse_float=Decimal)
            bin_timestamp = calendar.timegm(time.gmtime())
            data_timestamp = int(calendar.timegm(dateutil.parser.parse(dict_content["Head"]["Timestamp"]).timetuple()))
            energy_production_grid_alltime = self.datum_value(dict_content, ["Body", "Data", "0", "EnergyReal_WAC_Minus_Absolute"], factor=10)
            self.datum_push(
                "energy.production.grid",
                "current", "integral",
                energy_production_grid_alltime,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "alltime",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            energy_production_grid_day = energy_production_grid_alltime - \
                                         self.datum_get(DATUM_QUEUE_MIN, "energy.production.grid", "integral", "1", "alltime")["data_value"]
            self.datum_push(
                "energy.production.grid",
                "current", "integral",
                energy_production_grid_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_push(
                "energy.production.yield",
                "current", "integral",
                self.datum_value(energy_production_grid_day * Decimal(0.000007135), factor=100),
                "$",
                100,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            energy_production_inverter_day = self.datum_get(DATUM_QUEUE_LAST, "energy.production.inverter", "integral", "1", "alltime")[
                                                 "data_value"] - \
                                             self.datum_get(DATUM_QUEUE_MIN, "energy.production.inverter", "integral", "1", "alltime")["data_value"]
            self.datum_push(
                "energy.consumption.savings",
                "current", "integral",
                self.datum_value((energy_production_inverter_day - energy_production_grid_day) * Decimal(0.0000240673), factor=100),
                "$",
                100,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            energy_consumption_grid_alltime = self.datum_value(dict_content, ["Body", "Data", "0", "EnergyReal_WAC_Plus_Absolute"], factor=10)
            self.datum_push(
                "energy.consumption.grid",
                "current", "integral",
                energy_consumption_grid_alltime,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "alltime",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            energy_consumption_grid_day = energy_consumption_grid_alltime - \
                                          self.datum_get(DATUM_QUEUE_MIN, "energy.consumption.grid", "integral", "1", "alltime")["data_value"]
            self.datum_push(
                "energy.consumption.grid",
                "current", "integral",
                energy_consumption_grid_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_push(
                "energy.consumption.cost",
                "current", "integral",
                self.datum_value(Decimal(0.485989) + energy_consumption_grid_day * Decimal(0.0000240673), factor=100),
                "$",
                100,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0,
                data_derived_max=True,
                data_derived_min=True
            )
            self.datum_pop()
        except Exception:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.exception(
                    "Unexpected error processing response [{}]".format(text_content))
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info(
                "Plugin [{}] push_meter on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))

    def __init__(self, parent, name, config):
        super(Fronius, self).__init__(parent, name, config)
        self.poll_meter_iteration = POLL_METER_ITERATIONS
