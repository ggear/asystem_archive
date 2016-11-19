jasmine.DEFAULT_TIMEOUT_INTERVAL = 500;

describe('ANode', function () {

    it('construct new', function () {
        var thrown;
        try {
            ANode();
        } catch (error) {
            thrown = error;
        }
        expect(thrown).toBe('Anonde constructor not used');
    });

    it('connection default', function (done) {
        connectionTest(done, "http://localhost:8080/")
    });

    it('connection filter last', function (done) {
        connectionTest(done, "http://localhost:8080/?scope=last")
    });

});

connectionTest = function (done, url) {
    var anode = new ANode(url,
        function () {
            expect(anode.isConnected()).toBe(true);
            done();
        });
};
