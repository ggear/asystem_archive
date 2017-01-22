# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
import time

import numpy
import pandas
from twisted.trial.unittest import TestCase


class TestPandas(TestCase):
    # noinspection PyShadowingBuiltins,PyUnusedLocal

    def test_df_append(self):
        datums_df_number = 200
        datums_df_value_type = "int64"
        # datums_df_value_type = numpy.dtype(decimal.Decimal)
        datums_df = pandas.DataFrame(numpy.random.randint(0, high=10000, size=datums_df_number).astype(datums_df_value_type))
        datums_df.rename(columns={0: "data_value"}, inplace=True)
        datums_df["bin_timestamp"] = 1484538000
        datums_df["data_timestamp"] = 1484538001
        datums_df = datums_df[["bin_timestamp", "data_timestamp", "data_value"]]
        datums_df.set_index("bin_timestamp", inplace=True)
        datums_df.index = pandas.to_datetime(datums_df.index, unit="s")

        datums_df.info()
        print(datums_df.memory_usage())
        print("datums_size={} bytes".format(sys.getsizeof(datums_df) / datums_df_number))

        def f1():
            result = datums_df
            for i in range(19):
                result = result.append(datums_df)
            return result

        def f2():
            result = []
            for i in range(20):
                result.append(datums_df)
            return pandas.concat(result)

        start = time.time()
        df1 = f1()
        # print("f1={} ms".format((time.time() - start) * 1000))

        start = time.time()
        df2 = f2()
        # print("f2={} ms".format((time.time() - start) * 1000))

        datums_df.reset_index(inplace=True)
        datums_df["bin_timestamp"] = datums_df["bin_timestamp"].astype("int64") // 10 ** 9

        # print(datums_df)
        #
        # print(datums_df.to_csv(index=False))
        # print(datums_df.to_json(orient="columns"))
        # print(datums_df.to_dict(orient="dict"))
        #
        # print(datums_df.to_json(orient="records"))
        # print(datums_df.to_dict(orient="records"))

    # noinspection PyShadowingBuiltins
    def test_datum_to_df(self):
        datum1_1 = {"bin_timestamp": 1484538000, "data_timestamp": 1484538001, "data_value": 40, "data_scale": 10}
        datum1_2 = {"bin_timestamp": 1484538004, "data_timestamp": 1484538011, "data_value": 60, "data_scale": 10}
        datums1 = [datum1_1, datum1_2]

        datum2_1 = {"bin_timestamp": 1484538000, "data_timestamp": 1484538001, "data_value": 20, "data_scale": 10}
        datum2_2 = {"bin_timestamp": 1484538002, "data_timestamp": 1484538005, "data_value": 10, "data_scale": 10}
        datum2_3 = {"bin_timestamp": 1484538004, "data_timestamp": 1484538011, "data_value": 30, "data_scale": 10}
        datums2 = [datum2_1, datum2_2, datum2_3]

        datum3_1 = {"bin_timestamp": 1484538004, "data_timestamp": 1484538002, "data_value": 100, "data_scale": 10}
        datum3_2 = {"bin_timestamp": 1484538009, "data_timestamp": 1484538001, "data_value": 90, "data_scale": 10}
        datums3 = [datum3_1, datum3_2]

        def datum_to_df(datums, name):
            datums_df = pandas.DataFrame(datums)
            datums_df.set_index("bin_timestamp", inplace=True)
            datums_df.index = pandas.to_datetime(datums_df.index, unit="s")
            datums_df.rename(columns={"data_timestamp": "t_" + name}, inplace=True)
            datums_df["v_" + name] = datums_df["data_value"] / datums_df["data_scale"]
            del datums_df["data_value"]
            del datums_df["data_scale"]
            return datums_df

        metrics = [datum_to_df(datums1, "a"), datum_to_df(datums2, "b"), datum_to_df(datums3, "c")]
        metrics_value_cols = ["v_a", "v_b", "v_c"]

        pivot = "wide"
        scope = "history"
        format = "csv"
        period = 1  # None
        method = "mean"  # None "min" "max" "mean"
        fill = "all"  # None "linear" "zeros" "all"

        merge = metrics[0].join(metrics[1:], how="outer")
        print(merge)

        resample = merge
        if period is not None:
            resample = getattr(merge.resample(str(period) + "S"), "mean" if method is None else method)()
        print(resample)

        interpolate = resample
        if fill == "all" or fill == "linear":
            interpolate[metrics_value_cols] = resample[metrics_value_cols].interpolate()
        if fill == "all" or fill == "zeros":
            interpolate[metrics_value_cols] = interpolate[metrics_value_cols].fillna(0)
        print(interpolate)

        formated = interpolate
        formated.index = formated.index.astype(numpy.int64) // 10 ** 9
        formated.index.name = "bin_timestamp"
        print(formated)
