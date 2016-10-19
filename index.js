/**
 * GPS grab and store:
 * @param {Object} tdx Api object.
 * @param {Object} output functions.
 * @param {Object} packageParams of the databot.
 */
function GrapPark(tdxApi, output, packageParams) {
    var keyCount = 0;
    tdxApi.getDatasetData(packageParams.parkSources, null, null, null, function (errSources, sourcesData) {
        if (errSources) {
            output.error("Error Park sources table: %s", JSON.stringify(errSources));
            process.exit(1);
        } else {
            output.debug("Retrieved Park sources table: %d entries", sourcesData.data.length);

            var element;
            var idList = [];

            if (!sourcesData.data.length)
                return;
            else {

                // Pick the first element of the table
                // as only one API call is needed for this particulat dataset
                element = sourcesData.data[0];

                if (element.Src != 'MK' && element.Datatype != 'XML')
                    return;
            }

            _.map(sourcesData.data, function (val) {
                idList.push(val.LotCode);
            });

            var req = function (el, cb) {

                output.debug("Processing element Host:%s", el.Host);

                request
                    .get(el.Host + el.Path)
                    .auth(el.APIKey, '')
                    .end((error, response) => {
                        if (error) {
                            output.error("API request error: %s", error);
                            cb();
                        } else {
                            parseXmlString(response.text, function (errXmlParse, result) {
                                if (errXmlParse) {
                                    output.error("XML parse error: %s", JSON.stringify(errXmlParse));
                                    cb();
                                } else {
                                    var entryList = [];
                                    _.map(result.feed.datastream, function (val) {
                                        if (idList.indexOf(Number(val['$']['id'])) > -1) {
                                            var entry = {
                                                'ID': Number(val['$']['id']),
                                                'timestamp': Number(new Date(val.current_time[0]).getTime()),
                                                'currentvalue': Number(val.current_value[0]),
                                                'maxvalue': Number(val.max_value[0])
                                            };

                                            entryList.push(entry);
                                        }
                                    });

                                    var addRecurs = function (entryList, cbAdd) {
                                        if (!entryList.length)
                                            cbAdd();
                                        else {
                                            var one_element = entryList.pop();
                                            
                                            tdxApi.addDatasetData(packageParams.parkDataTable, one_element, function (errAdd, resAdd) {
                                                if (errAdd) {
                                                    output.error("Error adding entry to dataset: %s", JSON.stringify(errAdd));
                                                    addRecurs(entryList, cbAdd);
                                                } else {
                                                    output.debug("Added %s to the dataset", JSON.stringify(one_element));
                                                    output.result({ 'timestamp': Date.now(), 'value': one_element });
                                                    keyCount++;
                                                    addRecurs(entryList, cbAdd);
                                                }
                                            });
                                        }
                                    };
                                    addRecurs(entryList, cb)
                                }
                            });
                        }
                    });
            }

            var computing = false;
            var timer = setInterval(function () {
                if (!computing) {
                    computing = true;
                    req(element, function () { computing = false; });
                }
            }, packageParams.timerFrequency);
        }
    });
}

/**
 * Main databot entry function:
 * @param {Object} input schema.
 * @param {Object} output functions.
 * @param {Object} context of the databot.
 */
function databot(input, output, context) {
    "use strict"
    output.progress(0);

    var tdxApi = new TDXAPI({
        commandHost: context.commandHost,
        queryHost: context.queryHost,
        accessTokenTTL: context.packageParams.accessTokenTTL
    });

    tdxApi.authenticate(context.shareKeyId, context.shareKeySecret, function (err, accessToken) {
        if (err) {
            output.error("%s", JSON.stringify(err));
            process.exit(1);
        } else {
            GrapPark(tdxApi, output, context.packageParams);
        }
    });
}

var input;
var _ = require('lodash');
var request = require("superagent");
var parseXmlString = require('xml2js').parseString;
var TDXAPI = require("nqm-api-tdx");

if (process.env.NODE_ENV == 'test') {
    // Requires nqm-databot-gpsgrab.json file for testing
    input = require('./databot-test.js')(process.argv[2]);
} else {
    // Load the nqm input module for receiving input from the process host.
    input = require("nqm-databot-utils").input;
}

// Read any data passed from the process host. Specify we're expecting JSON data.
input.pipe(databot);