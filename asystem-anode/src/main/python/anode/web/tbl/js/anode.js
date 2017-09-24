(function () {
    var anodeConnector = tableau.makeConnector();
    anodeConnector.getSchema = function (schemaCallback) {
        var anodeColumns = [{
            id: "data_source",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "data_metric",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "data_temporal",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "data_type",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "data_value",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "data_unit",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "data_scale",
            dataType: tableau.dataTypeEnum.float
        }, {
            id: "data_timestamp",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "bin_timestamp",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "bin_width",
            dataType: tableau.dataTypeEnum.int
        }, {
            id: "bin_unit",
            dataType: tableau.dataTypeEnum.string
        }];
        var anodeSchema = {
            id: "anode",
            alias: "anode",
            columns: anodeColumns
        };
        schemaCallback([anodeSchema]);
    };

    anodeConnector.getData = function (table, doneCallback) {
        var uri = document.location.href;
        var uriBase = uri.substring(uri.indexOf("//") + 2, uri.length);
        var uriHostPort = uriBase.substring(0, uriBase.indexOf("/"));
        $.getJSON("http://" + uriHostPort + "/rest/?types=point&scope=history&format=json", function (anodeTable) {
            table.appendRows(anodeTable);
            doneCallback();
        });
    };

    tableau.registerConnector(anodeConnector);

    $(document).ready(function () {
        $("#submitButton").click(function () {
            tableau.connectionName = "ANode";
            tableau.submit();
        });
    });

})();
