# -*- coding: utf-8 -*-

from __future__ import print_function

import calendar
import json
import logging
import time
from decimal import Decimal

from anode.plugin.plugin import Plugin


class Davis(Plugin):
    def _push(self, text_content):
        # noinspection PyBroadException
        try:
            dict_content = json.loads(text_content, parse_float=Decimal)
            bin_timestamp = calendar.timegm(time.gmtime())
            if "packet" in dict_content:
                bin_unit = "second"
                bin_width = dict_content["packet"]["interval"]
                data_timestamp = dict_content["packet"]["dateTime"]
                self.datum_push(
                    "temperature.indoor.dining",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["inTemp"]) is None else self.datum_value(
                        (dict_content["packet"]["inTemp"] - 32) * 5 / 9 - 1, factor=10),
                    u"°C",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "dewpoint.indoor.dining",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["inDewpoint"]) is None else self.datum_value(
                        (dict_content["packet"]["inDewpoint"] - 32) * 5 / 9, factor=10),
                    u"°C",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "humidity.indoor.dining",
                    "current", "point",
                    self.datum_value(dict_content["packet"], ["inHumidity"]),
                    "%",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_upper=100,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "sun.outdoor.azimuth",
                    "current", "point",
                    self.datum_value(dict_content["packet"], ["sunaz"]),
                    u"°",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "sun.outdoor.altitude",
                    "current", "point",
                    self.datum_value(dict_content["packet"], ["sunalt"]),
                    u"°",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "sun.outdoor.rise",
                    "current", "epoch",
                    self.datum_value(dict_content["packet"], ["sunrise"]),
                    "scalor",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    1,
                    "day"
                )
                self.datum_push(
                    "sun.outdoor.set",
                    "current", "epoch",
                    self.datum_value(dict_content["packet"], ["sunset"]),
                    "scalor",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    1,
                    "day"
                )
                self.datum_push(
                    "temperature.outdoor.apparent",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["appTemp"]) is None else self.datum_value(
                        (dict_content["packet"]["appTemp"] - 32) * 5 / 9, factor=10),
                    u"°C",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "temperature.outdoor.roof",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["outTemp"]) is None else self.datum_value(
                        (dict_content["packet"]["outTemp"] - 32) * 5 / 9, factor=10),
                    u"°C",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "dewpoint.outdoor.roof",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["dewpoint"]) is None else self.datum_value(
                        (dict_content["packet"]["dewpoint"] - 32) * 5 / 9, factor=10),
                    u"°C",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "wind.outdoor.roof",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["windSpeed"]) is None else self.datum_value(
                        dict_content["packet"]["windSpeed"] * Decimal(1.60934)),
                    "km/h",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "windbearing.outdoor.roof",
                    "current", "point",
                    self.datum_value(dict_content["packet"], ["windDir"]),
                    u"°",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "windgust.outdoor.roof",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["windGust"]) is None else self.datum_value(
                        dict_content["packet"]["windGust"] * Decimal(1.60934)),
                    "km/h",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "windgustbearing.outdoor.roof",
                    "current", "point",
                    self.datum_value(dict_content["packet"], ["windGustDir"]),
                    u"°",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "windchill.outdoor.roof",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["windchill"]) is None else self.datum_value(
                        (dict_content["packet"]["windchill"] - 32) * 5 / 9, factor=10),
                    u"°C",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "pressure.outdoor.roof",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["barometer"]) is None else self.datum_value(
                        dict_content["packet"]["barometer"] * Decimal(33.8639)),
                    "mbar",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "heatindex.outdoor.roof",
                    "current", "point",
                    self.datum_value(dict_content["packet"], ["heatindex"]),
                    "scalor",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "humidity.outdoor.roof",
                    "current", "point",
                    self.datum_value(dict_content["packet"], ["outHumidity"]),
                    "%",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_upper=100,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "cloud.outdoor.base",
                    "current", "point",
                    None if self.datum_value(dict_content["packet"], ["cloudbase"]) is None else self.datum_value(
                        dict_content["packet"]["cloudbase"] * Decimal(0.3048)),
                    "m",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "rain.outdoor.roof",
                    "current", "integral",
                    None if self.datum_value(dict_content["packet"], ["monthRain"]) is None else self.datum_value(
                        dict_content["packet"]["monthRain"] * Decimal(2.54), factor=100),
                    "cm",
                    100,
                    data_timestamp,
                    bin_timestamp,
                    1,
                    "month",
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "rain.outdoor.roof",
                    "current", "integral",
                    None if self.datum_value(dict_content["packet"], ["yearRain"]) is None else self.datum_value(
                        dict_content["packet"]["yearRain"] * Decimal(2.54), factor=100),
                    "cm",
                    100,
                    data_timestamp,
                    bin_timestamp,
                    1,
                    "year",
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
            if "record" in dict_content:
                bin_unit = "minute"
                bin_width = dict_content["record"]["interval"]
                data_timestamp = dict_content["record"]["dateTime"]
                self.datum_push(
                    "rainrate.outdoor.roof",
                    "current", "mean",
                    None if self.datum_value(dict_content["record"], ["rainRate"]) is None else self.datum_value(
                        dict_content["record"]["rainRate"] * Decimal(25.4), factor=10),
                    "mm/h",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "rain.outdoor.roof",
                    "current", "integral",
                    None if self.datum_value(dict_content["record"], ["rain"]) is None else self.datum_value(
                        dict_content["record"]["rain"] * Decimal(25.4), factor=10),
                    "mm",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    bin_width,
                    bin_unit,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
            self.datum_pop()
        except Exception:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.exception(
                    "Unexpected error processing response [{}]".format(text_content))

    def __init__(self, parent, name, config):
        super(Davis, self).__init__(parent, name, config)
        self.last_push = calendar.timegm(time.gmtime())
