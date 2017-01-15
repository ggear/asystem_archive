# -*- coding: utf-8 -*-

from __future__ import print_function

import pandas
from twisted.trial.unittest import TestCase


class TestPandas(TestCase):
    # noinspection PyShadowingBuiltins,PyUnusedLocal
    def test_main_default(self):
        metric1 = pandas.DataFrame({"timestamp": [1, 4], "a": [4, 6]})
        metric1.set_index("timestamp", inplace=True)
        metric1.index = pandas.to_datetime(metric1.index, unit="s")
        # print(metric1)

        metric2 = pandas.DataFrame({"timestamp": [1, 2, 4], "b": [2, 1, 3]})
        metric2.set_index("timestamp", inplace=True)
        metric2.index = pandas.to_datetime(metric2.index, unit="s")
        # print(metric2)

        metric3 = pandas.DataFrame({"timestamp": [4, 10], "c": [9, 1]})
        metric3.set_index("timestamp", inplace=True)
        metric3.index = pandas.to_datetime(metric3.index, unit="s")
        # print(metric3)

        metrics = [metric1, metric2, metric3]

        pivot = "wide"
        scope = "history"
        format = "csv"
        period = 60  # None
        method = "mean"  # None "min" "max"
        fill = "all"  # None "linear" "zeros"

        merge = metrics[0].join(metrics[1:], how="outer")
        print(merge)

        resample = merge if period is None else getattr(merge.resample(str(period) + "S"), "mean" if method is None else method)()
        print(resample)

        interpolate = resample if fill is None else resample.interpolate() if fill == "linear" or fill == "all" else resample
        interpolate = interpolate if fill is None else interpolate.fillna(0) if fill == "zeros" or fill == "all" else interpolate
        print(interpolate)
