/**
 * GPS grab and store:
 * @param {Object} tdx Api object.
 * @param {Object} output functions.
 * @param {Object} packageParams of the databot.
 */
function GrabPark(tdxApi, output, packageParams) {
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
                request
                    .get(el.Host + el.Path)
                    .auth(el.APIKey, '')
                    .end((error, response) => {
                        if (error) {
                            output.error("API request error: %s", JSON.stringify(error));
                            cb();
                        } else {
                            var entryList = [];
                            parseXmlStringAsync(response.text)
                                .then((result) => {
                                    _.forEach(result.feed.datastream, (val) => {
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

                                    return tdxApi.updateDatasetDataAsync(packageParams.parkDataTable, entryList, true);
                                })
                                .then((res) => {
                                    return tdxApi.updateDatasetDataAsync(packageParams.parkDataTableLatest, entryList, true);
                                })
                                .catch((err) => {
                                    // TDX API error or XML parse error.
                                    output.error("Failure processing entries: %s", err.message);
                                    return ({ error: true });
                                })
                                .then((res) => {
                                    // Finish execution
                                    output.debug("Saved %d entries to parkDataTable", entryList.length);
                                    return cb(res);
                                });
                        }
                    });
            }
        
            var computing = false;
            var timer = setInterval(()=>{
                if (!computing) {
                    computing = true;
                    req(element, (res)=>{
                        computing = false;
                    });
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

    Promise.promisifyAll(tdxApi);

    tdxApi.authenticate(context.shareKeyId, context.shareKeySecret, function (err, accessToken) {
        if (err) {
            output.error("%s", JSON.stringify(err));
            process.exit(1);
        } else {
            GrabPark(tdxApi, output, context.packageParams);
        }
    });
}

var input;
var _ = require('lodash');
var request = require("superagent");
var Promise = require("bluebird");
var xml2js = require('xml2js');
var TDXAPI = require("nqm-api-tdx");
var parseXmlString;

Promise.promisifyAll(xml2js);
parseXmlStringAsync = xml2js.parseStringAsync;

if (process.env.NODE_ENV == 'test') {
    // Requires nqm-databot-gpsgrab.json file for testing
    input = require('./databot-test.js')(process.argv[2]);
} else {
    // Load the nqm input module for receiving input from the process host.
    input = require("nqm-databot-utils").input;
}

// Read any data passed from the process host. Specify we're expecting JSON data.
input.pipe(databot);