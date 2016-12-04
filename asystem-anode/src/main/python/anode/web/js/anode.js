var ANode = function (uri, onopen, onclose, onmessage) {

    if (!(this instanceof ANode)) {
        throw "Anonde constructor not used";
    }

    var outer = this;
    var uriBase = uri.substring(uri.indexOf("//") + 2, uri.length);
    var uriHostPort = uriBase.substring(0, uriBase.indexOf("/"));
    var uriParameters = uriBase.substring(uriBase.indexOf("/") + 1, uriBase.length);
    var webSocketUri = "ws://" + uriHostPort + "/ws/" + uriParameters;

    this.connected = false;
    this.webSocket = new WebSocket(webSocketUri);

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

    this.isConnected = function () {
        return this.connected;
    };

    this.toDatum = function (datumJson) {
        var datum = JSON.parse(datumJson);
        datum.ui_id = "datums_" + (datum.data_metric + datum.data_type + datum.bin_width + datum.bin_unit).replace(/\./g, "").toLowerCase();
        datum.data_timeliness = "";
        if (datum.data_type == "point" || datum.bin_width == 0) {
            datum.data_timeliness = "now";
        }
        else {
            if (datum.data_type != "integral" && datum.data_type != "enumeration" && datum.data_type != "epoch") {
                datum.data_timeliness = datum.data_type + " ";
            }
            if (datum.bin_width > 1) {
                if (datum.data_temporal == "forecast") {
                    if (datum.bin_unit == "day") {
                        if (datum.bin_width == 2) {
                            datum.data_timeliness += "tommorrow";
                        } else if (datum.bin_width == 3) {
                            datum.data_timeliness += "the day after tommorrow";
                        }
                    } else if (datum.bin_unit == "daytime") {
                        if (datum.bin_width == 2) {
                            datum.data_timeliness += "tommorrow day time";
                        } else if (datum.bin_width == 3) {
                            datum.data_timeliness += "the day after tommorrow day time";
                        }
                    } else if (datum.bin_unit == "nighttime") {
                        if (datum.bin_width == 2) {
                            datum.data_timeliness += "tommorrow night time";
                        } else if (datum.bin_width == 3) {
                            datum.data_timeliness += "the day after tommorrow night time";
                        }
                    }

                } else {
                    datum.data_timeliness += "over the last " + datum.bin_width + " " + datum.bin_unit + "s";
                }
            } else {
                if (datum.bin_unit == "day") {
                    datum.data_timeliness += "today";
                } else if (datum.bin_unit == "daytime") {
                    datum.data_timeliness += "during the day";
                } else if (datum.bin_unit == "nighttime") {
                    datum.data_timeliness += "over night";
                } else if (datum.bin_unit == "alltime") {
                    datum.data_timeliness += "for all time";
                } else {
                    datum.data_timeliness += "this " + datum.bin_unit;
                }
            }
        }
        return datum;
    }

};
