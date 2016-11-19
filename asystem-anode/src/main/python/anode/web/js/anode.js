var ANode = function (uri, onopen, onclose, onmessage) {

    if (!(this instanceof ANode)) {
        throw 'Anonde constructor not used';
    }

    var outer = this;
    var uriBase = uri.substring(uri.indexOf("//") + 2, uri.length);
    var uriHostPort = uriBase.substring(0, uriBase.indexOf("/"));
    var uriParameters = uriBase.substring(uriBase.indexOf("/") + 1, uriBase.length);
    var webSocketUri = "ws://" + uriHostPort + "/push/" + uriParameters;

    this.connected = false;
    this.webSocket = new WebSocket(webSocketUri);

    this.webSocket.onopen = function () {
        outer.connected = true;
        console.log("WebSocket connection opened");
        if (typeof onopen !== 'undefined') {
            try {
                onopen();
            }
            catch (error) {
                console.log(error)
            }
        }
    };

    this.webSocket.onmessage = function (frame) {
        if (typeof frame.data == "string") {
            console.log("WebSocket message received");
        }
        if (typeof onmessage !== 'undefined') {
            try {
                onmessage(frame.data);
            }
            catch (error) {
                console.log(error)
            }
        }
    };

    this.webSocket.onclose = function () {
        outer.connected = false;
        console.log("WebSocket connection closed");
        if (typeof onclose !== 'undefined') {
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

};
