module.exports = (configpath) => {
    "use strict"

    var debugLog = require("debug")("nqm-databot");
    var util = require("util");
    var fs = require("fs");
    var assert = require("assert");
    var _ = require("lodash");
    var TDXAPI = require("nqm-api-tdx");
    var Promise = require("bluebird");

    var config = require(configpath);

    var outputType = {
        DEBUG: 1, // STDOUT - diagnostic fed back to TDX
        ERROR: 2, // STDERR - fed back to TDX
        RESULT: 3, // Result update to the TDX
        PROGRESS: 4, // Progress updates to TDX
    };

    var _writeOutput = function (fd, msg) {
        msg = typeof msg !== "undefined" ? msg : "";
        var buf = new Buffer(msg.toString());
        fs.writeSync(fd, buf, 0, buf.length);
    };

    var writeDebug = function () {
        var msg = util.format.apply(util, arguments);
        return debugLog(msg);
    };

    var writeError = function () {
        var msg = util.format.apply(util, arguments);
        return _writeOutput(outputType.ERROR, msg + "\n");
    };

    var writeResult = function (obj) {
        if (typeof obj !== "object") {
            return writeError("output.result - expected type 'object', got type '%s'", typeof obj);
        } else {
            return debugLog(JSON.stringify(obj) + "\n");
        }
    };

    var writeProgress = function (progress) {
        assert(_.isNumber(progress));
        return _writeOutput(outputType.DEBUG, "Progress:"+progress.toString() + "\n");
    };


    var context;
    var output = {
        debug: writeDebug,
        progress: writeProgress,
        error: writeError,
        result: writeResult
    };

    var readAndRun = function (cb) {
        if (typeof cb !== "function") {
            throw new Error("input.read - callback required");
        }
        var context = {
            "instanceId":config.instanceId,
            "instanceName":config.instanceName,
            "instancePort":config.instancePort,
            "instanceAuthKey":config.instanceAuthKey,
            "authToken":config.authToken,
            "outputSchema":config.outputSchema,
            "chunkNumber":config.chunkNumber,
            "chunkTotal":config.chunkTotal,
            "packageParams":config.packageParams,
            "commandHost":config.commandHost,
            "queryHost":config.queryHost,
            "tdxApi":null,
            "shareKeyId":config.shareKeyId,
            "shareKeySecret":config.shareKeySecret
        };

        // Initialise a tdx api instance
        context.tdxApi = new TDXAPI({
            commandHost: context.commandHost,
            queryHost: context.queryHost,
            accessToken: context.authToken
        });
        Promise.promisifyAll(context.tdxApi);

        context.tdxApi.authenticate(config.shareKeyId, config.shareKeySecret, function(err, accessToken){
            if(err) throw err;
            else {
                context.authToken = accessToken;
                cb(config.inputSchema, output, context);
            }
        });
    }

    return {
        pipe: readAndRun
    };
}