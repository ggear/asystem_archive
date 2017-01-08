WEB_PORT = 8091;
ANODE_WARMUP_PERIOD = 5000;
jasmine.DEFAULT_TIMEOUT_INTERVAL = 10000;


describe('ANode', function () {

    var metrics;
    var metrics_anode;

    beforeAll(function (done) {
        metrics = 0;
        metrics_anode = 0;
        setTimeout(function () {
            new ANode(connectionUri("")).restfulRequest("metrics=anode", function (datums) {
                for (var i = 0; i < datums.length; i++) {
                    metrics++;
                    if (datums[i].data_metric.indexOf("metrics", this.length - "metrics".length) !== -1) {
                        metrics_anode += datums[i].data_value;
                    }
                }
                done();
            });
        }, ANODE_WARMUP_PERIOD);
    });

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

    it('message metrics', function (done) {
        messageTest(done, connectionUri("metrics=power.production.inverter"), 3)
    });

    it('message bins', function (done) {
        messageTest(done, connectionUri("bins=1second"), 9)
    });

    it('message metrics', function (done) {
        messageTest(done, connectionUri("metrics=power"), 21)
    });

    it('message scope history', function (done) {
        messageTest(done, connectionUri("scope=history"), metrics - metrics_anode)
    });

    it('message limit', function (done) {
        messageTest(done, connectionUri("limit=" + metrics), metrics)
    });

    it('message limit', function (done) {
        messageTest(done, connectionUri("limit=" + (metrics * 2)), metrics)
    });

    it('message limit', function (done) {
        messageTest(done, connectionUri("limit=some_nonnumeric_limit"), metrics)
    });

    it('message scope last', function (done) {
        messageTest(done, connectionUri("scope=last"), metrics)
    });

    it('message something ', function (done) {
        messageTest(done, connectionUri("something=else"), metrics)
    });

    it('message', function (done) {
        messageTest(done, connectionUri(""), metrics)
    });

});

connectionUri = function (parameters) {
    return "http://localhost:" + WEB_PORT + "/" + (parameters ? ("?" + parameters) : "");
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
