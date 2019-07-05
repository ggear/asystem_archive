import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# TODO: Replace with real plugin

HUE_BRIDGE_IP = "192.168.2.197"
HUE_BRIDGE_TOKEN = "o6PRIGF-uz17Gbp8JSWG1haIAAmnPVA-Zv7b3a9S"

groups = requests.get("https://" + HUE_BRIDGE_IP + "/api/" + HUE_BRIDGE_TOKEN + "/groups", verify=False).json()
lights = requests.get("https://" + HUE_BRIDGE_IP + "/api/" + HUE_BRIDGE_TOKEN + "/lights", verify=False).json()
for index in groups:
    if groups[index]["state"]["any_on"]:
        reachable = True
        for light in groups[index]["lights"]:
            reachable = reachable and lights[index]["state"]["reachable"]
        print("{} {}".format(groups[index]["name"], "ON" if reachable else "OFF"))
