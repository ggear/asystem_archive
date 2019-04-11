from __future__ import print_function

import calendar
import json
import logging
from decimal import Decimal

import datetime
import dateutil.parser
import treq

import anode
from anode.plugin.plugin import Plugin

HTTP_TIMEOUT = 10


class Darksky(Plugin):

    def _poll(self):
        self.http_get("https://api.darksky.net/forecast/fd02f0c75b26923e31ccc08e5b7e1022/-31.915,116.106", self.push_forecast)

    # noinspection PyShadowingNames
    def http_get(self, url, callback):
        connection_pool = self.config["pool"] if "pool" in self.config else None
        treq.get(url, timeout=HTTP_TIMEOUT, pool=connection_pool).addCallbacks(
            lambda response, url=url, callback=callback: self.http_response(response, url, callback),
            errback=lambda error, url=url: anode.Log(logging.ERROR).log("Plugin", "error",
                                                                        lambda: "[{}] error processing HTTP GET [{}] with [{}]".format(
                                                                            self.name, url, error.getErrorMessage())))

    def http_response(self, response, url, callback):
        if response.code == 200:
            treq.text_content(response).addCallbacks(callback)
        else:
            anode.Log(logging.ERROR).log("Plugin", "error",
                                         lambda: "[{}] error processing HTTP response [{}] with [{}]".format(self.name, url, response.code))

    def push_forecast(self, content):
        log_timer = anode.Log(logging.DEBUG).start()
        # noinspection PyBroadException
        try:
            dict_content = json.loads(content, parse_float=Decimal)
            bin_timestamp = self.get_time()
            data_timestamp = dict_content["currently"]["time"]
            if self.get_time_period(dict_content["daily"]["data"][0]["time"], 24 * 60 * 60) == \
                    self.get_time_period(bin_timestamp, 24 * 60 * 60):
                for forecast_index in range(3):
                    forecast = dict_content["daily"]["data"][forecast_index]
                    self.datum_push(
                        "conditions__forecast__glen_Dforrest",
                        "forecast", "enumeration",
                        0,
                        "__",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index + 1,
                        "day",
                        data_string=self.datum_value(forecast, ["icon"]).lower().replace("-", " ").encode("ascii", "ignore")
                    )
                    self.datum_push(
                        "temperature__forecast__glen_Dforrest",
                        "forecast", "point",
                        None if self.datum_value(forecast, ["temperatureHigh"]) is None else \
                            int((self.datum_value(forecast, ["temperatureHigh"]) - 32) * 5 / 9 * 10),
                        "_PC2_PB0C",
                        10,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index + 1,
                        "day",
                        data_derived_max=forecast_index == 0
                    )
                    self.datum_push(
                        "temperature__forecast__glen_Dforrest",
                        "forecast", "low",
                        None if self.datum_value(forecast, ["temperatureLow"]) is None else \
                            int((self.datum_value(forecast, ["temperatureLow"]) - 32) * 5 / 9 * 10),
                        "_PC2_PB0C",
                        10,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index + 1,
                        "day"
                    )

                    # TODO: Replace rain with precipitation data
                    # self.datum_push(
                    #     "rain__forecast__glen_Dforrest",
                    #     "forecast", "integral",
                    #     self.datum_value(forecast, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_allday", "mm"]),
                    #     "mm",
                    #     1,
                    #     data_timestamp,
                    #     bin_timestamp,
                    #     forecast_index + 1,
                    #     "day",
                    #     data_bound_lower=0,
                    #     data_derived_max=forecast_index == 0,
                    #     data_derived_min=forecast_index == 0
                    # )
                    # self.datum_push(
                    #     "rain__forecast__glen_Dforrest",
                    #     "forecast", "integral",
                    #     self.datum_value(forecast, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_day", "mm"]),
                    #     "mm",
                    #     1,
                    #     data_timestamp,
                    #     bin_timestamp,
                    #     forecast_index + 1,
                    #     "day_Dtime",
                    #     data_bound_lower=0,
                    #     data_derived_max=forecast_index == 0,
                    #     data_derived_min=forecast_index == 0
                    # )
                    # self.datum_push(
                    #     "rain__forecast__glen_Dforrest",
                    #     "forecast", "integral",
                    #     self.datum_value(forecast, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_night", "mm"]),
                    #     "mm",
                    #     1,
                    #     data_timestamp,
                    #     bin_timestamp,
                    #     forecast_index + 1,
                    #     "night_Dtime",
                    #     data_bound_lower=0,
                    #     data_derived_max=forecast_index == 0,
                    #     data_derived_min=forecast_index == 0
                    # )

                    self.datum_push(
                        "wind__forecast__glen_Dforrest",
                        "forecast", "mean",
                        None if self.datum_value(forecast, ["windSpeed"]) is None else \
                            int(self.datum_value(forecast, ["windSpeed"], factor=10) * 1.609344),
                        "km_P2Fh",
                        10,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index + 1,
                        "day",
                        data_bound_lower=0,
                        data_derived_min=forecast_index == 0
                    )
                    self.datum_push(
                        "wind__forecast__glen_Dforrest",
                        "forecast", "high",
                        None if self.datum_value(forecast, ["windGust"]) is None else \
                            int(self.datum_value(forecast, ["windGust"], factor=10) * 1.609344),
                        "km_P2Fh",
                        10,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index + 1,
                        "day",
                        data_bound_lower=0
                    )
                    self.datum_push(
                        "humidity__forecast__glen_Dforrest",
                        "forecast", "mean",
                        None if self.datum_value(forecast, ["humidity"]) is None else \
                            int(self.datum_value(forecast, ["humidity"], factor=100)),
                        "_P25",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index + 1,
                        "day",
                        data_bound_upper=100,
                        data_bound_lower=0,
                        data_derived_max=forecast_index == 0,
                        data_derived_min=forecast_index == 0
                    )
            self.publish()
        except Exception as exception:
            anode.Log(logging.ERROR).log("Plugin", "error", lambda: "[{}] error [{}] processing response:\n{}"
                                         .format(self.name, exception, content), exception)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.push_forecast)
