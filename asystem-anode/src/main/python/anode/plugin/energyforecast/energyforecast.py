from __future__ import print_function

import logging
import os

import time
import datetime
import pandas
import treq
import xmltodict
from twisted_s3 import auth

import anode
from anode.plugin.plugin import DATUM_QUEUE_LAST
from anode.plugin.plugin import Plugin

HTTP_TIMEOUT = 10

S3_REGION = "ap-southeast-2"
S3_BUCKET = "asystem-amodel"

MODEL_PRODUCTION_VERSION = "1001"


class Energyforecast(Plugin):
    def _poll(self):
        self.http_get(S3_REGION, S3_BUCKET, "/", "list-type=2&max-keys=1000000&prefix=asystem", self.list_models)

    def http_get(self, region, bucket, path, params, callback):
        host = bucket + ".s3-" + region + ".amazonaws.com"
        url = "http://" + host + path + "?" + params
        payload = auth.compute_hashed_payload(b"")
        timestamp = datetime.datetime.utcnow()
        headers = {"host": host, "x-amz-content-sha256": payload, "x-amz-date": timestamp.strftime(auth.ISO8601_FMT)}
        headers["Authorization"] = auth.compute_auth_header(headers, "GET", timestamp, region, bucket, path, params, payload,
                                                            os.environ["AWS_ACCESS_KEY"], os.environ["AWS_SECRET_KEY"])
        connection_pool = self.config["pool"] if "pool" in self.config else None
        treq.get(url, headers=headers, timeout=HTTP_TIMEOUT, pool=connection_pool).addCallbacks(
            lambda response, url=url, callback=callback: self.http_response(response, url, callback),
            errback=lambda error, url=url: anode.Log(logging.ERROR).log("Plugin", "error",
                                                                        lambda: "[{}] error processing HTTP GET [{}] with [{}]".format(
                                                                            self.name, url, error.getErrorMessage())))

    def http_response(self, response, url, callback):
        if response.code == 200:
            treq.content(response).addCallbacks(callback, callbackKeywords={"url": url})
        else:
            anode.Log(logging.ERROR).log("Plugin", "error",
                                         lambda: "[{}] error processing HTTP response [{}] with [{}]".format(self.name, url, response.code))

    def list_models(self, content, url):
        log_timer = anode.Log(logging.DEBUG).start()
        try:
            for key_remote in xmltodict.parse(content)["ListBucketResult"]["Contents"]:
                path_remote = "s3://" + S3_BUCKET + "/" + key_remote["Key"].encode("utf-8")
                path_status = self.pickled_status(os.path.join(self.config["db_dir"], "model"),
                                                  path_remote, model_lower=MODEL_PRODUCTION_VERSION)
                if not path_status[0] and path_status[1]:
                    self.http_get(S3_REGION, S3_BUCKET, "/" + path_remote.replace("s3://" + S3_BUCKET + "/", ""), "", self.pull_model)
                elif not path_status[0]:
                    self.push_forecast(path_remote)
        except Exception as exception:
            anode.Log(logging.ERROR).log("Plugin", "error", lambda: "[{}] error [{}] processing response:\n{}"
                                         .format(self.name, exception, content), exception)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.list_models)

    def pull_model(self, content, url):
        log_timer = anode.Log(logging.DEBUG).start()
        try:
            path_remote = url[:-1]
            self.pickled_put(os.path.join(self.config["db_dir"], "model"), path_remote, content)
            self.push_forecast(path_remote)
        except Exception as exception:
            anode.Log(logging.ERROR).log("Plugin", "error", lambda: "[{}] error [{}] processing binary response of length [{}]"
                                         .format(self.name, exception, len(content)), exception)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.pull_model)

    def push_forecast(self, path=None):
        log_timer = anode.Log(logging.DEBUG).start()
        try:
            bin_timestamp = self.get_time()
            models = self.pickled_get(os.path.join(self.config["db_dir"], "model"), path=path)
            if self.name in models and \
                    self.anode.get_plugin("davis") is not None and \
                    self.anode.get_plugin("wunderground") is not None:
                energy_production_today = self.anode.get_plugin("fronius").datum_get(
                    DATUM_QUEUE_LAST, "energy__production__inverter", "integral", "Wh", 1, "day")
                energy_production_today = energy_production_today["data_value"] / energy_production_today["data_scale"] \
                    if energy_production_today is not None else None
                sun_rise = self.anode.get_plugin("davis").datum_get(
                    DATUM_QUEUE_LAST, "sun__outdoor__rise", "epoch", "scalar", 1, "day")
                sun_rise = sun_rise["data_value"] / sun_rise["data_scale"] \
                    if sun_rise is not None else None
                sun_set = self.anode.get_plugin("davis").datum_get(
                    DATUM_QUEUE_LAST, "sun__outdoor__set", "epoch", "scalar", 1, "day")
                sun_set = sun_set["data_value"] / sun_set["data_scale"] \
                    if sun_set is not None else None
                sun_azimuth = self.anode.get_plugin("davis").datum_get(
                    DATUM_QUEUE_LAST, "sun__outdoor__azimuth", "point", "_PC2_PB0", 2, "second")
                sun_azimuth = sun_azimuth["data_value"] / sun_azimuth["data_scale"] \
                    if sun_azimuth is not None else None
                sun_altitude = self.anode.get_plugin("davis").datum_get(
                    DATUM_QUEUE_LAST, "sun__outdoor__altitude", "point", "_PC2_PB0", 2, "second")
                sun_altitude = sun_altitude["data_value"] / sun_altitude["data_scale"] \
                    if sun_altitude is not None else None
                for day in range(1, 4):
                    temperature_forecast = self.anode.get_plugin("wunderground").datum_get(
                        DATUM_QUEUE_LAST, "temperature__forecast__glen_Dforrest", "point", "_PC2_PB0C", day, "day")
                    temperature_forecast = temperature_forecast["data_value"] / temperature_forecast["data_scale"] \
                        if temperature_forecast is not None else None
                    rain_forecast = self.anode.get_plugin("wunderground").datum_get(
                        DATUM_QUEUE_LAST, "rain__forecast__glen_Dforrest", "integral", "mm", day, "day")
                    rain_forecast = rain_forecast["data_value"] / rain_forecast["data_scale"] \
                        if rain_forecast is not None else None
                    humidity_forecast = self.anode.get_plugin("wunderground").datum_get(
                        DATUM_QUEUE_LAST, "humidity__forecast__glen_Dforrest", "mean", "_P25", day, "day")
                    humidity_forecast = humidity_forecast["data_value"] / humidity_forecast["data_scale"] \
                        if humidity_forecast is not None else None
                    wind_forecast = self.anode.get_plugin("wunderground").datum_get(
                        DATUM_QUEUE_LAST, "wind__forecast__glen_Dforrest", "mean", "km_P2Fh", day, "day")
                    wind_forecast = wind_forecast["data_value"] / wind_forecast["data_scale"] \
                        if wind_forecast is not None else None
                    conditions_forecast = self.anode.get_plugin("wunderground").datum_get(
                        DATUM_QUEUE_LAST, "conditions__forecast__glen_Dforrest", "enumeration", "__", day, "day")
                    conditions_forecast = conditions_forecast["data_string"] \
                        if conditions_forecast is not None else None
                    features_dict = {
                        "datum__bin__date": datetime.datetime.fromtimestamp(bin_timestamp + (86400 * (day - 1))).strftime('%Y/%m/%d'),
                        "energy__production__inverter": 0,
                        "temperature__forecast__glen_Dforrest": temperature_forecast,
                        "rain__forecast__glen_Dforrest": rain_forecast,
                        "humidity__forecast__glen_Dforrest": humidity_forecast,
                        "wind__forecast__glen_Dforrest": wind_forecast,
                        "sun__outdoor__rise": sun_rise,
                        "sun__outdoor__set": sun_set,
                        "sun__outdoor__azimuth": sun_azimuth,
                        "sun__outdoor__altitude": sun_altitude,
                        "conditions__forecast__glen_Dforrest": conditions_forecast
                    }
                    features_df = pandas.DataFrame([features_dict], columns=features_dict.keys()).apply(pandas.to_numeric, errors='ignore')
                    for model_version in models[self.name]:
                        model = models[self.name][model_version][1]
                        energy_production_forecast = 0
                        model_classifier = "" if model_version == MODEL_PRODUCTION_VERSION else ("_D" + model_version)
                        try:
                            energy_production_forecast = model['execute'](model=model, features=model['execute'](
                                features=features_df, engineering=True), prediction=True)[0]
                        except Exception as exception:
                            anode.Log(logging.ERROR).log("Plugin", "error", lambda: "[{}] error [{}] executing model [{}]"
                                                         .format(self.name, exception, path), exception)
                        self.datum_push(
                            "energy__production_Dforecast" + model_classifier + "__inverter",
                            "forecast", "integral",
                            self.datum_value(energy_production_forecast, factor=10),
                            "Wh",
                            10,
                            bin_timestamp,
                            bin_timestamp,
                            day,
                            "day",
                            asystem_version=models[self.name][model_version][0],
                            data_version=model_version,
                            data_bound_lower=0)
                        if day == 1:
                            if model_classifier == "":
                                self.datum_push(
                                    "energy__production_Dforecast_Ddaylight__inverter",
                                    "forecast", "integral",
                                    self.datum_value(((time.time() - time.mktime(datetime.date.today().timetuple())) / (sun_set - sun_rise))
                                                     if (
                                            sun_set is not None and sun_rise is not None and (sun_set - sun_rise) != 0) else 0),
                                    "_P25",
                                    1,
                                    bin_timestamp,
                                    bin_timestamp,
                                    day,
                                    "day",
                                    asystem_version=models[self.name][model_version][0],
                                    data_version=model_version,
                                    data_bound_lower=0,
                                    data_bound_upper=100)
                            else:
                                self.datum_push(
                                    "energy__production_Dforecast_Daccuracy" + model_classifier + "__inverter",
                                    "forecast", "integral",
                                    self.datum_value((energy_production_forecast / energy_production_today)
                                                     if (energy_production_today is not None and energy_production_today != 0) else 0),
                                    "_P25",
                                    1,
                                    bin_timestamp,
                                    bin_timestamp,
                                    day,
                                    "day",
                                    asystem_version=models[self.name][model_version][0],
                                    data_version=model_version,
                                    data_bound_lower=0,
                                    data_bound_upper=100,
                                    data_derived_max=True)
                self.publish()
        except Exception as exception:
            anode.Log(logging.ERROR).log("Plugin", "error", lambda: "[{}] error [{}] processing model [{}]"
                                         .format(self.name, exception, path), exception)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.push_forecast)

    def __init__(self, parent, name, config, reactor):
        super(Energyforecast, self).__init__(parent, name, config, reactor)
        self.pickled_get(os.path.join(self.config["db_dir"], "model"), name=self.name, warm=True)
        for datum_metric, datum in self.datums.items():
            if datum_metric <= ("energy__production_Dforecast_D" + MODEL_PRODUCTION_VERSION + "__inverter"):
                del self.datums[datum_metric]
