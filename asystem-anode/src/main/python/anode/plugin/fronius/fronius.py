# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function

import json
import logging

import calendar
import datetime
import dateutil.parser
import treq
from decimal import Decimal

import anode
from anode.plugin.plugin import DATUM_QUEUE_LAST
from anode.plugin.plugin import DATUM_QUEUE_MIN
from anode.plugin.plugin import Plugin

HTTP_TIMEOUT = 5
POLL_METER_ITERATIONS = 5

FEE_ACCESS = 0.4859888
TARIFF_FEED_IN = 0.000007135
TARIFF_PEAK = 0.000055
TARIFF_OFF_PEAK = 0.00001485
TARIFF_FLAT = 0.00002647403

HOUR_PEAK_START = 15
HOUR_PEAK_FINISH = 21


# noinspection PyBroadException
class Fronius(Plugin):
    def _poll(self):
        self.http_get("http://10.0.1.203/solar_api/v1/GetPowerFlowRealtimeData.fcgi", self.push_flow)
        if self.is_clock or self.poll_meter_iteration == POLL_METER_ITERATIONS:
            self.poll_meter_iteration = 0
            self.http_get("http://10.0.1.203/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System", self.push_meter)
        else:
            self.poll_meter_iteration += 1

    # noinspection PyShadowingNames
    def http_get(self, url, callback):
        connection_pool = self.config["pool"] if "pool" in self.config else None
        treq.get(url, timeout=HTTP_TIMEOUT, pool=connection_pool).addCallbacks(
            lambda response, url=url, callback=callback: self.http_response(response, url, callback),
            errback=lambda error, url=url: anode.Log(logging.ERROR)
                .log("Plugin", "error", lambda: "[{}] error processing HTTP GET [{}] with [{}]".format(self.name, url, error.getErrorMessage())))

    def http_response(self, response, url, callback):
        if response.code == 200:
            treq.text_content(response).addCallbacks(callback)
        else:
            anode.Log(logging.ERROR).log("Plugin", "error",
                                         lambda: "[{}] error processing HTTP response [{}] with [{}]".format(self.name, url, response.code))

    def push_flow(self, text_content):
        log_timer = anode.Log(logging.DEBUG).start()
        try:
            dict_content = json.loads(text_content, parse_float=Decimal)
            bin_timestamp = self.get_time()
            data_timestamp = int(calendar.timegm(dateutil.parser.parse(dict_content["Head"]["Timestamp"]).timetuple()))
            self.datum_push(
                "power.export.grid",
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
                self.datum_value(dict_content, ["Body", "Data", "Site", "E_Year"], factor=10),
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "year",
                data_bound_lower=0,
                data_derived_min=True
            )
            energy_production_inverter_alltime = self.datum_value(dict_content, ["Body", "Data", "Site", "E_Total"], factor=10)
            self.datum_push(
                "energy.production.inverter",
                "current", "integral",
                energy_production_inverter_alltime,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "all-time",
                data_bound_lower=0,
                data_derived_min=True
            )
            energy_production_inverter_alltime_min = self.datum_get(DATUM_QUEUE_MIN,
                                                                    "energy.production.inverter", "integral", "Wh", 1, "all-time", 1, "day")
            energy_production_inverter_day = (energy_production_inverter_alltime - energy_production_inverter_alltime_min["data_value"]) \
                if energy_production_inverter_alltime_min is not None else 0
            self.datum_push(
                "energy.production.inverter",
                "current", "integral",
                energy_production_inverter_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            self.publish()
        except Exception as exception:
            anode.Log(logging.ERROR).log("Plugin", "error", lambda: "[{}] error [{}] processing response:\n"
                                         .format(self.name, exception), exception)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.push_flow)

    def push_meter(self, text_content):
        log_timer = anode.Log(logging.DEBUG).start()
        try:
            dict_content = json.loads(text_content, parse_float=Decimal)
            bin_timestamp = self.get_time()
            data_timestamp = int(calendar.timegm(dateutil.parser.parse(dict_content["Head"]["Timestamp"]).timetuple()))
            energy_export_grid_alltime = self.datum_value(dict_content, ["Body", "Data", "0", "EnergyReal_WAC_Minus_Absolute"], factor=10)
            self.datum_push(
                "energy.export.grid",
                "current", "integral",
                energy_export_grid_alltime,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "all-time",
                data_bound_lower=0,
                data_derived_min=True
            )
            energy_export_grid_alltime_min = self.datum_get(DATUM_QUEUE_MIN, "energy.export.grid", "integral", "Wh", 1, "all-time", 1, "day")
            energy_export_grid_day = (energy_export_grid_alltime - energy_export_grid_alltime_min["data_value"]) \
                if energy_export_grid_alltime_min is not None else 0
            self.datum_push(
                "energy.export.grid",
                "current", "integral",
                energy_export_grid_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            energy_production_inverter_alltime_last = self.datum_get(DATUM_QUEUE_LAST,
                                                                     "energy.production.inverter", "integral", "Wh", "1", "all-time")
            energy_production_inverter_alltime_min = self.datum_get(DATUM_QUEUE_MIN,
                                                                    "energy.production.inverter", "integral", "Wh", 1, "all-time", 1, "day")
            energy_production_inverter_day = (energy_production_inverter_alltime_last["data_value"] -
                                              energy_production_inverter_alltime_min["data_value"]) \
                if (energy_production_inverter_alltime_last is not None and energy_production_inverter_alltime_min is not None) else 0
            energy_consumption_inverter_day = energy_production_inverter_day - energy_export_grid_day
            self.datum_push(
                "energy.consumption.inverter",
                "current", "integral",
                energy_consumption_inverter_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
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
                "all-time",
                data_bound_lower=0,
                data_derived_min=True
            )
            energy_consumption_grid_min = self.datum_get(DATUM_QUEUE_MIN, "energy.consumption.grid", "integral", "Wh", 1, "all-time", 1, "day")
            energy_consumption_grid_day = (energy_consumption_grid_alltime - energy_consumption_grid_min["data_value"]) \
                if energy_consumption_grid_min is not None else 0
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
                data_bound_lower=0
            )
            energy_consumption_peak_morning_inverter = self.datum_get(DATUM_QUEUE_LAST,
                                                                      "energy.consumption-peak-morning.inverter", "integral", "Wh", 1, "all-time")
            energy_consumption_peak_morning_grid = self.datum_get(DATUM_QUEUE_LAST,
                                                                  "energy.consumption-peak-morning.grid", "integral", "Wh", 1, "all-time")
            if energy_consumption_peak_morning_grid is not None and \
                            self.get_time_period(energy_consumption_peak_morning_grid["data_timestamp"], Plugin.get_seconds(1, "day")) != \
                            self.get_time_period(bin_timestamp, Plugin.get_seconds(1, "day")):
                energy_consumption_peak_morning_grid = None
            if energy_consumption_peak_morning_grid is None and bin_timestamp >= \
                    (self.get_time_period(bin_timestamp, Plugin.get_seconds(1, "day")) + HOUR_PEAK_START * 60 * 60):
                self.datum_push(
                    "energy.consumption-peak-morning.grid",
                    "derived", "integral",
                    energy_consumption_grid_alltime,
                    "Wh",
                    10,
                    bin_timestamp,
                    bin_timestamp,
                    1,
                    "all-time",
                    data_bound_lower=0
                )
                self.datum_push(
                    "energy.consumption-peak-morning.inverter",
                    "derived", "integral",
                    energy_production_inverter_day - energy_export_grid_day,
                    "Wh",
                    10,
                    bin_timestamp,
                    bin_timestamp,
                    1,
                    "all-time",
                    data_bound_lower=0
                )
            energy_consumption_peak_evening_inverter = self.datum_get(DATUM_QUEUE_LAST,
                                                                      "energy.consumption-peak-evening.inverter", "integral", "Wh", 1, "all-time")
            energy_consumption_peak_evening_grid = self.datum_get(DATUM_QUEUE_LAST,
                                                                  "energy.consumption-peak-evening.grid", "integral", "Wh", 1, "all-time")
            if energy_consumption_peak_evening_grid is not None and \
                            self.get_time_period(energy_consumption_peak_evening_grid["data_timestamp"], Plugin.get_seconds(1, "day")) != \
                            self.get_time_period(bin_timestamp, Plugin.get_seconds(1, "day")):
                energy_consumption_peak_evening_grid = None
            if energy_consumption_peak_evening_grid is None and bin_timestamp >= \
                    (self.get_time_period(bin_timestamp, Plugin.get_seconds(1, "day")) + HOUR_PEAK_FINISH * 60 * 60):
                self.datum_push(
                    "energy.consumption-peak-evening.grid",
                    "derived", "integral",
                    energy_consumption_grid_alltime,
                    "Wh",
                    10,
                    bin_timestamp,
                    bin_timestamp,
                    1,
                    "all-time",
                    data_bound_lower=0
                )
                self.datum_push(
                    "energy.consumption-peak-evening.inverter",
                    "derived", "integral",
                    energy_production_inverter_day - energy_export_grid_day,
                    "Wh",
                    10,
                    bin_timestamp,
                    bin_timestamp,
                    1,
                    "all-time",
                    data_bound_lower=0
                )
            energy_consumption_grid_off_peak_morning_day = 0
            if energy_consumption_peak_morning_grid is None and energy_consumption_peak_evening_grid is None:
                energy_consumption_grid_off_peak_morning_day = (energy_consumption_grid_alltime - energy_consumption_grid_min["data_value"]) \
                    if energy_consumption_grid_min is not None else 0
            if energy_consumption_peak_morning_grid is not None:
                energy_consumption_grid_off_peak_morning_day = \
                    (energy_consumption_peak_morning_grid["data_value"] - energy_consumption_grid_min["data_value"]) \
                        if energy_consumption_grid_min is not None else 0
            self.datum_push(
                "energy.consumption-off-peak-morning.grid",
                "current", "integral",
                energy_consumption_grid_off_peak_morning_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            energy_consumption_grid_peak_day = 0
            if energy_consumption_peak_morning_grid is not None and energy_consumption_peak_evening_grid is None:
                energy_consumption_grid_peak_day = energy_consumption_grid_alltime - energy_consumption_peak_morning_grid["data_value"]
            if energy_consumption_peak_morning_grid is not None and energy_consumption_peak_evening_grid is not None:
                energy_consumption_grid_peak_day = energy_consumption_peak_evening_grid["data_value"] - \
                                                   energy_consumption_peak_morning_grid["data_value"]
            self.datum_push(
                "energy.consumption-peak.grid",
                "current", "integral",
                energy_consumption_grid_peak_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            energy_consumption_grid_off_peak_evening_day = 0
            if energy_consumption_peak_morning_grid is not None and energy_consumption_peak_evening_grid is not None:
                energy_consumption_grid_off_peak_evening_day = energy_consumption_grid_alltime - energy_consumption_peak_evening_grid["data_value"]
            self.datum_push(
                "energy.consumption-off-peak-evening.grid",
                "current", "integral",
                energy_consumption_grid_off_peak_evening_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            energy_consumption_grid_off_peak_day = energy_consumption_grid_off_peak_morning_day + energy_consumption_grid_off_peak_evening_day
            self.datum_push(
                "energy.consumption-off-peak.grid",
                "current", "integral",
                energy_consumption_grid_off_peak_day,
                "Wh",
                10,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            self.datum_push(
                "energy.export.yield",
                "current", "integral",
                self.datum_value(energy_export_grid_day * Decimal(TARIFF_FEED_IN), factor=100),
                "$",
                100,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            energy_consumption_savings_peak_day = 0
            energy_consumption_savings_off_peak_day = 0
            if energy_consumption_peak_morning_grid is None and energy_consumption_peak_evening_grid is None:
                energy_consumption_savings_off_peak_day = energy_production_inverter_day - energy_export_grid_day
            elif energy_consumption_peak_morning_grid is not None and energy_consumption_peak_evening_grid is None:
                energy_consumption_savings_off_peak_day = energy_consumption_peak_morning_inverter["data_value"]
                energy_consumption_savings_peak_day = energy_production_inverter_day - energy_export_grid_day - \
                                                      energy_consumption_peak_morning_inverter["data_value"]
            elif energy_consumption_peak_morning_grid is not None and energy_consumption_peak_evening_grid is not None:
                energy_consumption_savings_off_peak_day = energy_production_inverter_day - energy_export_grid_day - \
                                                          energy_consumption_peak_evening_inverter["data_value"] + \
                                                          energy_consumption_peak_morning_inverter["data_value"]
                energy_consumption_savings_peak_day = energy_consumption_peak_evening_inverter["data_value"] - \
                                                      energy_consumption_peak_morning_inverter["data_value"]
            self.datum_push(
                "energy.consumption.savings",
                "current", "integral",
                self.datum_value(energy_consumption_savings_peak_day *
                                 Decimal(TARIFF_PEAK if datetime.datetime.fromtimestamp(bin_timestamp).weekday() < 5 else TARIFF_OFF_PEAK) +
                                 energy_consumption_savings_off_peak_day * Decimal(TARIFF_OFF_PEAK), factor=100),
                "$",
                100,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            self.datum_push(
                "energy.consumption.cost-home",
                "current", "integral",
                self.datum_value(Decimal(FEE_ACCESS) + energy_consumption_grid_day * Decimal(TARIFF_FLAT), factor=100),
                "$",
                100,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            self.datum_push(
                "energy.consumption.cost-solar",
                "current", "integral",
                self.datum_value(Decimal(FEE_ACCESS) + energy_consumption_grid_peak_day *
                                 Decimal(TARIFF_PEAK if datetime.datetime.fromtimestamp(bin_timestamp).weekday() < 5 else TARIFF_OFF_PEAK) +
                                 energy_consumption_grid_off_peak_day * Decimal(TARIFF_OFF_PEAK), factor=100),
                "$",
                100,
                data_timestamp,
                bin_timestamp,
                1,
                "day",
                data_bound_lower=0
            )
            self.publish()
        except Exception as exception:
            anode.Log(logging.ERROR).log("Plugin", "error", lambda: "[{}] error [{}] processing response:\n{}"
                                         .format(self.name, exception, text_content), exception)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.push_meter)

    def __init__(self, parent, name, config, reactor):
        super(Fronius, self).__init__(parent, name, config, reactor)
        self.poll_meter_iteration = POLL_METER_ITERATIONS
