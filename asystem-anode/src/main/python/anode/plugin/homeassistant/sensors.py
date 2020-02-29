# -*- coding: utf-8 -*-

import json
import time

import paho.mqtt.client as mqtt
import yaml
import sys
import os

MODE = "QUERY"

CONFIG = None
TIME_WAIT_SECS = 2
CSV_ROWS = [",".join(["ID", "Name", "Domain", "Group", "Location", "Topic", "Metadata"])]


# TODO
#   - Write out sensors CSV, test name, location, domain, group - filter out max/min/counters, rename metrics as necessary
#   - Merge into Lovelace UI, perhaps do manually based on cohorts
#   - Merge into Grafana UI, perhaps do automatically based on cohorts


def on_connect(client, user_data, flags, return_code):
    client.subscribe("{}/#".format(CONFIG["publish_push_metadata_topic"]))


def on_message(client, user_data, message):
    try:
        topic = message.topic.encode('utf-8')
        if len(message.payload) > 0:
            payload = message.payload.decode('unicode-escape').encode('utf-8')
            payload_json = json.loads(payload)
            payload_unicode = "\"" + payload.replace("\"", "\"\"") + "\""
            payload_csv = ",".join([
                payload_json["unique_id"].encode('utf-8'),
                payload_json["name"].encode('utf-8'),
                payload_json["json_attributes"][0].encode('utf-8'),
                payload_json["json_attributes"][1].encode('utf-8'),
                payload_json["json_attributes"][2].encode('utf-8'),
                topic,
                payload_unicode,
            ])
            CSV_ROWS.append(payload_csv)
            if MODE == "DELETE":
                client.publish(topic, payload=None, qos=1, retain=True)
    except Exception as exception:
        print(exception)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "delete":
        MODE = "DELETE"
    with open(os.path.dirname(os.path.realpath(__file__)) + "/../../../config/anode.yaml", "r") as stream:
        CONFIG = yaml.load(stream)
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(CONFIG["publish_host"], CONFIG["publish_port"], 60)
        time_start = time.time()
        while True:
            client.loop()
            time_elapsed = time.time() - time_start
            if time_elapsed > TIME_WAIT_SECS:
                client.disconnect()
                break
        with open('sensors.csv', 'w') as file:
            for line in CSV_ROWS:
                file.write(line)
                file.write('\n')
        print("{} [{}] sensors".format("DELETED" if MODE == "DELETE" else "DETECTED", len(CSV_ROWS) - 1))
