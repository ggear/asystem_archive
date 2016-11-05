from __future__ import print_function

import calendar
import io
import os
import time
from uuid import getnode as get_mac

import avro
import avro.io
import avro.schema
from twisted.trial.unittest import TestCase


class ANodeTestAvro(TestCase):
    # noinspection PyMethodMayBeStatic
    def test_avro(self):
        schema = avro.schema.parse(open(os.path.dirname(__file__) + "/../model/datum.avsc", "rb").read())
        writer = io.BytesIO()
        avro.io.DatumWriter(schema).write(
            {
                "anode_id": format(get_mac(), "x").decode("hex"),
                "data_source": "fronius",
                "data_metric": "power.production.grid",
                "data_type": "point",
                "data_value": 10,
                "data_unit": "W",
                "data_scale": 10,
                "data_timestamp": calendar.timegm(time.gmtime()) - 1,
                "bin_timestamp": calendar.timegm(time.gmtime()),
                "bin_width": 30,
                "bin_unit": "s"
            },
            avro.io.BinaryEncoder(writer))
        datum_bytes = writer.getvalue()
        reader = avro.io.DatumReader(schema)
        datum_dict = reader.read(avro.io.BinaryDecoder(io.BytesIO(datum_bytes)))
        print("Serialised: {}, Deserialised: {}".format(len(datum_bytes), len(str(datum_dict))))
