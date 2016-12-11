METRICS_TOTAL = 137;
WEBSOCKET_PORT = 8091;
jasmine.DEFAULT_TIMEOUT_INTERVAL = 5000;

describe('ANode', function () {

    it('connection', function (done) {
        connectionTest(done, connectionUri(""))
    });

    it('message limit', function (done) {
        messageTest(done, connectionUri("limit=1"), 1)
    });

    it('message limit limit', function (done) {
        messageTest(done, connectionUri("limit=some_nonnumeric_limit&limit=1"), 1)
    });

    it('message metrics bins', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter&bins=1second"), 1)
    });

    it('message metrics types', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter&types=point"), 1)
    });

    it('message metrics types bins', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter&types=point&bins=1second"), 1)
    });

    it('message metrics metrics types bins', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter&metrics=&types=point&bins=1second"), 1)
    });

    it('message metrics metrics types types bins', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter&metrics=&types=point&types=some_nonexistant_type&bins=1second"), 1)
    });

    it('message metrics metrics types types bins bins', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter&metrics=&types=point&bins=1second&bins=some_nonexistant_bin"), 1)
    });

    it('message limit', function (done) {
        messageTest(done, connectionUri("limit=2"), 2)
    });

    it('message metrics metrics types bins', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter&metrics=power.production.grid&metrics=&types=point&bins=1second"), 2)
    });

    it('message metrics metrics types bins sources', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter&metrics=power.production.grid&metrics=&types=point&bins=1second&sources=fronius"), 2)
    });

    it('message metrics', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter"), 3)
    });

    it('message bins', function (done) {
        messageTest(done, connectionUri("bins=1second"), 9)
    });

    it('message metrics', function (done) {
        messageTest(done, connectionUri("metrics=power"), 27)
    });

    it('message sources', function (done) {
        messageTest(done, connectionUri("sources=fronius"), 32)
    });

    it('message limit', function (done) {
        messageTest(done, connectionUri("limit=" + METRICS_TOTAL), METRICS_TOTAL)
    });

    it('message limit', function (done) {
        messageTest(done, connectionUri("limit=" + (METRICS_TOTAL * 2)), METRICS_TOTAL)
    });

    it('message limit', function (done) {
        messageTest(done, connectionUri("limit=some_nonnumeric_limit"), METRICS_TOTAL)
    });

    it('message scope last', function (done) {
        messageTest(done, connectionUri("scope=last"), METRICS_TOTAL)
    });

    it('message scope history', function (done) {
        messageTest(done, connectionUri("scope=history"), METRICS_TOTAL)
    });

    it('message something ', function (done) {
        messageTest(done, connectionUri("something=else"), METRICS_TOTAL)
    });

    it('message', function (done) {
        messageTest(done, connectionUri(""), METRICS_TOTAL)
    });

});

connectionUri = function (parameters) {
    return "http://localhost:" + WEBSOCKET_PORT + "/" + (parameters ? ("?" + parameters) : "");
};

connectionTest = function (done, url) {
    var anode = new ANode(url,
        function () {
            expect(anode.isConnected()).toBe(true);
            done();
        });
};

messageTest = function (done, url, messagesExpected) {
    var messagesReceived = 0;
    new ANode(url,
        function () {
        },
        function () {
        },
        function (datum) {
            expect(datum).not.toEqual(null);
            expect(datum.data_metric).not.toEqual(null);
            expect(datum.data_metric).not.toEqual("");
            if (++messagesReceived >= messagesExpected)
                done();
        });
};
