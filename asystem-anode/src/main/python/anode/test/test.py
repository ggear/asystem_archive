import calendar
import io
import os
import sys
import time
from uuid import getnode as get_mac

import avro
import avro.io
import avro.schema
from anode.anode import main
from mock import patch
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase


# noinspection PyUnresolvedReferences,PyMethodMayBeStatic
class ANodeTest(TestCase):
    # noinspection PyPep8Naming
    # noinspection PyAttributeOutsideInit
    def setUp(self):
        self.clock = Clock()

    def tick_tock(self, period, periods):
        for tickTock in range(0, period * periods, period):
            self.clock.advance(period)

    def test_main_default(self):
        with patch.object(sys, 'argv', ["anode"]):
            main(self.clock, lambda: self.tick_tock(1, 3))

    def test_main_quiet_short(self):
        with patch.object(sys, 'argv', ["anode", "-q"]):
            main(self.clock, lambda: self.tick_tock(1, 1))

    def test_main_quiet_long(self):
        with patch.object(sys, 'argv', ["anode", "--quiet"]):
            main(self.clock, lambda: self.tick_tock(1, 1))

    def test_avro(self):
        schema = avro.schema.parse(open(os.path.dirname(__file__) + "/../model/datum.avsc", "rb").read())
        writer = io.BytesIO()
        avro.io.DatumWriter(schema).write(
            {
                "anode": format(get_mac(), "x").decode("hex"),
                "source": "fronius",
                "metric": "energy.production.grid",
                "type": "point",
                "bin_width": 30,
                "bin_unit": "s",
                "bin_timestamp": calendar.timegm(time.gmtime()),
                "data_timestamp": calendar.timegm(time.gmtime()) - 1,
                "data_unit": "kW",
                "data_value": 10
            },
            avro.io.BinaryEncoder(writer))
        datum_bytes = writer.getvalue()
        print("Serialised to [{}] bytes".format(len(datum_bytes)))
        reader = avro.io.DatumReader(schema)
        datum_dict = reader.read(avro.io.BinaryDecoder(io.BytesIO(datum_bytes)))
        print("Deserialized to [{}] bytes".format(len(str(datum_dict))))
        print(datum_dict['anode'])
