function ANode(uri, onopen, onclose, onmessage) {

    if (!(this instanceof ANode)) {
        throw "Anonde constructor not used";
    }

    var outer = this;
    var uriBase = uri.substring(uri.indexOf("//") + 2, uri.length);
    var uriHostPort = uriBase.substring(0, uriBase.indexOf("/"));
    var uriParameters = uriBase.substring(uriBase.indexOf("/") + 1, uriBase.length);

    this.restfulUri = "http://" + uriHostPort + "/rest/";
    this.metadataUri = "http://" + uriHostPort + "/js/model/datum.avsc";
    this.webSocketUri = "ws://" + uriHostPort + "/ws/" + uriParameters;

    this.metadata = this.metadataRequest();
    this.orderingIndexes = {};
    this.orderingIndexes["data_metric"] = {};
    var data_metric = this.metadata[2]["symbols"];
    for (var i = 1; i < data_metric.length; i++) {
        this.orderingIndexes["data_metric"][data_metric[i]] = i * 100000;
    }
    this.orderingIndexes["data_type"] = {};
    var data_type = this.metadata[4]["symbols"];
    for (var i = 1; i < data_type.length; i++) {
        this.orderingIndexes["data_type"][data_type[i]] = i * 10000;
    }
    this.orderingIndexes["bin_unit"] = {};
    var bin_unit = this.metadata[6]["symbols"];
    for (var i = 1; i < bin_unit.length; i++) {
        this.orderingIndexes["bin_unit"][bin_unit[i]] = i * 1000;
    }

    this.connected = false;
    this.webSocket = new WebSocket(this.webSocketUri);

    this.webSocket.onopen = function () {
        outer.connected = true;
        if (typeof onopen !== "undefined") {
            try {
                onopen();
            }
            catch (error) {
                console.log(error)
            }
        }
    };

    this.webSocket.onmessage = function (frame) {
        if (typeof onmessage !== "undefined") {
            try {
                onmessage(outer.toDatum(frame.data));
            }
            catch (error) {
                console.log(error)
            }
        }
    };

    this.webSocket.onclose = function () {
        outer.connected = false;
        if (typeof onclose !== "undefined") {
            try {
                onclose();
            }
            catch (error) {
                console.log(error)
            }
        }
    };

    this.toDatum = function (datumJson) {
        var datum = JSON.parse(datumJson);
        datum.ui_id_sans_bin = "datums_" + (datum.data_metric + "_").replace(/\//g, "_").replace(/\./g, "_").toLowerCase();
        datum.ui_id = (datum.ui_id_sans_bin + datum.bin_width + "_" + datum.bin_unit).replace(/\//g, "_").replace(/\./g, "_").toLowerCase();
        datum.order_index = this.orderingIndexes["data_metric"][datum.data_metric] + this.orderingIndexes["data_type"][datum.data_type] +
            this.orderingIndexes["bin_unit"][datum.bin_unit] + datum.bin_width;
        datum.data_timeliness = "";
        if (((datum.data_temporal == "current" || datum.data_temporal == "repeat" || datum.data_temporal == "derived") && datum.data_type == "point") || datum.bin_width == 0) {
            datum.data_timeliness = "now";
        }
        else {
            if (datum.data_type != "point" && datum.data_type != "integral" && datum.data_type != "enumeration" && datum.data_type != "epoch") {
                datum.data_timeliness = datum.data_type + " ";
            }
            if (datum.bin_width > 1) {
                if (datum.data_temporal == "forecast") {
                    if (datum.bin_unit == "day") {
                        if (datum.bin_width == 2) {
                            datum.data_timeliness += "tommorrow";
                        } else if (datum.bin_width == 3) {
                            datum.data_timeliness += "ubermorrow";
                        }
                    } else if (datum.bin_unit == "day-time") {
                        if (datum.bin_width == 2) {
                            datum.data_timeliness += "tommorrow day time";
                        } else if (datum.bin_width == 3) {
                            datum.data_timeliness += "ubermorrow day time";
                        }
                    } else if (datum.bin_unit == "night-time") {
                        if (datum.bin_width == 2) {
                            datum.data_timeliness += "tommorrow night time";
                        } else if (datum.bin_width == 3) {
                            datum.data_timeliness += "ubermorrow night time";
                        }
                    }
                } else {
                    datum.data_timeliness += "over the last " + datum.bin_width + " " + datum.bin_unit + "s";
                }
            } else {
                if (datum.bin_unit == "day") {
                    datum.data_timeliness += "today";
                } else if (datum.bin_unit == "day-time") {
                    datum.data_timeliness += "during the day";
                } else if (datum.bin_unit == "night-time") {
                    datum.data_timeliness += "over night";
                } else if (datum.bin_unit == "all-time") {
                    datum.data_timeliness += "for all time";
                } else {
                    datum.data_timeliness += "this " + datum.bin_unit;
                }
            }
        }
        return datum;
    }

}

ANode.prototype.isConnected = function () {
    return this.connected;
};

ANode.prototype.metadataRequest = function () {
    var metadata = null;
    var metadataHttpRequest = new XMLHttpRequest();
    metadataHttpRequest.open("GET", this.metadataUri, false);
    metadataHttpRequest.onreadystatechange = function () {
        if (metadataHttpRequest.readyState == 4) {
            if (metadataHttpRequest.status == 200) {
                metadata = JSON.parse(metadataHttpRequest.responseText);
            }
        }
    };
    metadataHttpRequest.send();
    return metadata;
};

ANode.prototype.restfulRequest = function (parameters, onmessage) {
    var restfulHttpRequest = new XMLHttpRequest();
    restfulHttpRequest.open("GET", this.restfulUri + (parameters ? ("?" + parameters) : ""), true);
    restfulHttpRequest.onreadystatechange = function () {
        if (restfulHttpRequest.readyState == 4) {
            if (restfulHttpRequest.status == 200) {
                onmessage(JSON.parse(restfulHttpRequest.responseText));
            }
        }
    };
    restfulHttpRequest.send();
};

