/* ============================================================
 * node-okex-api
 https://github.com/ZhiweiWang/node-okex-api
 * ============================================================
 * Copyright 2018-, Zhiwei Wang
 * Released under the GPL 3.0 License
 * ============================================================ */

module.exports = (function() {
    "use strict";
    const WebSocket = require("ws");
    const request = require("request");
    const crypto = require("crypto");
    const file = require("fs");
    const stringHash = require("string-hash");
    const md5 = require("md5");
    const _ = require("underscore");
    const util = require("util");
    const VError = require("verror");
    const base = "https://www.okex.com/api/v1/";
    const stream = "wss://real.okex.com:10441/websocket";
    const userAgent = "Mozilla/4.0 (compatible; Node OKEx API)";
    const contentType = "application/x-www-form-urlencoded";
    let subscriptions = {};
    const default_options = {
        timeout: 30000,
        reconnect: true,
        verbose: false,
        test: false,
        log: function() {
            console.log(Array.prototype.slice.call(arguments));
        }
    };
    let options = default_options;
    let socketHeartbeatInterval;

    const publicRequest = function(method, params, callback) {
        var functionName = "publicRequest()";

        if (!_.isObject(params)) {
            var error = new VError(
                "%s second parameter %s must be an object. If no params then pass an empty object {}",
                functionName,
                params
            );
            return callback(error);
        }

        if (!callback || typeof callback != "function") {
            var error = new VError(
                "%s third parameter needs to be a callback function with err and data parameters",
                functionName
            );
            return callback(error);
        }

        var url = `${base}${method}.do`;

        var req_options = {
            url: url,
            method: "GET",
            headers: {
                "User-Agent": userAgent,
                "Content-type": contentType
            },
            timeout: options.timeout,
            qs: params,
            json: {} // request will parse the json response into an object
        };

        var requestDesc = util.format(
            "%s request to url %s with parameters %s",
            req_options.method,
            req_options.url,
            JSON.stringify(params)
        );

        executeRequest(req_options, requestDesc, callback);
    };

    const executeRequest = function(req_options, requestDesc, callback) {
        var functionName = "OKEX.executeRequest()";

        request(req_options, function(err, response, data) {
            var error = null, // default to no errors
                returnObject = data;

            if (err) {
                error = new VError(err, "%s failed %s", functionName, requestDesc);
                error.name = err.code;
            } else if (response.statusCode < 200 || response.statusCode >= 300) {
                error = new VError(
                    "%s HTTP status code %s returned from %s",
                    functionName,
                    response.statusCode,
                    requestDesc
                );
                error.name = response.statusCode;
            } else if (req_options.form) {
                try {
                    returnObject = JSON.parse(data);
                } catch (e) {
                    error = new VError(e, "Could not parse response from server: " + data);
                }
            } else if (req_options.json && !_.isObject(data)) {
                // if json request was not able to parse json response into an object
                error = new VError(
                    "%s could not parse response from %s\nResponse: %s",
                    functionName,
                    requestDesc,
                    data
                );
            }

            if (_.has(returnObject, "error_code")) {
                var errorMessage = mapErrorMessage(returnObject.error_code);

                error = new VError(
                    '%s %s returned error code %s, message: "%s"',
                    functionName,
                    requestDesc,
                    returnObject.error_code,
                    errorMessage
                );

                error.name = returnObject.error_code;
            }

            callback(error, returnObject);
        });
    };
    /**
     * Maps the OKEX error codes to error message
     * @param  {Integer}  error_code   OKEX error code
     * @return {String}                error message
     */
    const mapErrorMessage = function(error_code) {
        var errorCodes = {
            10000: "Required parameter can not be null",
            10001: "Requests are too frequent",
            10002: "System Error",
            10003: "Restricted list request, please try again later",
            10004: "IP restriction",
            10005: "Key does not exist",
            10006: "User does not exist",
            10007: "Signatures do not match",
            10008: "Illegal parameter",
            10009: "Order does not exist",
            10010: "Insufficient balance",
            10011: "Order is less than minimum trade amount",
            10012: "Unsupported symbol (not btc_usd or ltc_usd)",
            10013: "This interface only accepts https requests",
            10014: "Order price must be between 0 and 1,000,000",
            10015: "Order price differs from current market price too much",
            10016: "Insufficient coins balance",
            10017: "API authorization error",
            10026: "Loan (including reserved loan) and margin cannot be withdrawn",
            10027: "Cannot withdraw within 24 hrs of authentication information modification",
            10028: "Withdrawal amount exceeds daily limit",
            10029: "Account has unpaid loan, please cancel/pay off the loan before withdraw",
            10031: "Deposits can only be withdrawn after 6 confirmations",
            10032: "Please enabled phone/google authenticator",
            10033: "Fee higher than maximum network transaction fee",
            10034: "Fee lower than minimum network transaction fee",
            10035: "Insufficient BTC/LTC",
            10036: "Withdrawal amount too low",
            10037: "Trade password not set",
            10040: "Withdrawal cancellation fails",
            10041: "Withdrawal address not approved",
            10042: "Admin password error",
            10100: "User account frozen",
            10216: "Non-available API",
            503: "Too many requests (Http)"
        };

        if (!errorCodes[error_code]) {
            return "Unknown OKEX error code: " + error_code;
        }

        return errorCodes[error_code];
    };
    ////////////////////////////
    // reworked Tuitio's heartbeat code into a shared single interval tick
    const noop = function() {};
    const socketHeartbeat = function() {
        // sockets removed from `subscriptions` during a manual terminate()
        // will no longer be at risk of having functions called on them
        for (let endpointId in subscriptions) {
            const ws = subscriptions[endpointId];
            if (ws.isAlive) {
                ws.isAlive = false;
                if (ws.readyState === WebSocket.OPEN) ws.ping(noop);
            } else {
                if (options.verbose) options.log("Terminating inactive/broken WebSocket: " + ws.endpoint);
                if (ws.readyState === WebSocket.OPEN) ws.terminate();
            }
        }
    };
    const _handleSocketOpen = function(opened_callback) {
        this.isAlive = true;
        if (Object.keys(subscriptions).length === 0) {
            socketHeartbeatInterval = setInterval(socketHeartbeat, 30000);
        }
        subscriptions[this.endpoint] = this;
        if (typeof opened_callback === "function") opened_callback(this.endpoint);
    };
    const _handleSocketClose = function(reconnect, code, reason) {
        delete subscriptions[this.endpoint];
        if (Object.keys(subscriptions).length === 0) {
            clearInterval(socketHeartbeatInterval);
        }
        options.log(
            "WebSocket closed: " + this.endpoint + (code ? " (" + code + ")" : "") + (reason ? " " + reason : "")
        );
        if (options.reconnect && this.reconnect && reconnect) {
            if (parseInt(this.endpoint.length, 10) === 60) options.log("Account data WebSocket reconnecting...");
            else options.log("WebSocket reconnecting: " + this.endpoint + "...");
            try {
                reconnect();
            } catch (error) {
                options.log("WebSocket reconnect error: " + error.message);
            }
        }
    };
    const _handleSocketError = function(error) {
        // Errors ultimately result in a `close` event.
        // see: https://github.com/websockets/ws/blob/828194044bf247af852b31c49e2800d557fedeff/lib/websocket.js#L126
        options.log(
            "WebSocket error: " +
                this.endpoint +
                (error.code ? " (" + error.code + ")" : "") +
                (error.message ? " " + error.message : "")
        );
    };
    const _handleSocketHeartbeat = function() {
        this.isAlive = true;
    };
    const subscribe = function(endpoint, callback, reconnect = false, opened_callback = false) {
        if (options.verbose) options.log("Subscribed to " + endpoint);
        const ws = new WebSocket(stream);
        ws.reconnect = options.reconnect;
        ws.endpoint = endpoint;
        ws.isAlive = false;
        ws.on("open", _handleSocketOpen.bind(ws, opened_callback));
        ws.on("pong", _handleSocketHeartbeat);
        ws.on("error", _handleSocketError);
        ws.on("close", _handleSocketClose.bind(ws, reconnect));
        ws.on("message", function(data) {
            try {
                callback(JSON.parse(data));
            } catch (error) {
                options.log("Parse error: " + error.message);
            }
        });
        return ws;
    };
    const subscribeCombined = function(streams, callback, reconnect = false, opened_callback = false) {
        const queryParams = streams.join("/");
        const ws = new WebSocket(stream);
        ws.reconnect = options.reconnect;
        ws.endpoint = stringHash(queryParams);
        ws.streams = streams;
        ws.isAlive = false;
        if (options.verbose) options.log("CombinedStream: Subscribed to [" + ws.endpoint + "] " + queryParams);
        ws.on("open", _handleSocketOpen.bind(ws, opened_callback));
        ws.on("pong", _handleSocketHeartbeat);
        ws.on("error", _handleSocketError);
        ws.on("close", _handleSocketClose.bind(ws, reconnect));
        ws.on("message", function(data) {
            try {
                callback(JSON.parse(data));
            } catch (error) {
                options.log("CombinedStream: Parse error: " + error.message);
            }
        });
        return ws;
    };
    const addChannel = function(endpoint) {
        const ws = subscriptions[endpoint];
        if (ws.hasOwnProperty("streams")) {
            let channels = [];
            for (let channel of ws.streams) {
                channels.push({ event: "addChannel", channel });
            }
            ws.send(JSON.stringify(channels));
        } else {
            let channel = { event: "addChannel", channel: endpoint };
            ws.send(JSON.stringify(channel));
        }
    };

    const isArrayUnique = function(array) {
        let s = new Set(array);
        return s.size == array.length;
    };
    ////////////////////////////
    return {
        candlesticks: function(symbol, type, callback, options = { size: 500 }) {
            if (!callback) return;
            let params = Object.assign({ symbol, type }, options);

            publicRequest("kline", params, callback);
        },
        setOption: function(key, value) {
            options[key] = value;
        },
        options: function(opt, callback = false) {
            if (typeof opt === "string") {
                // Pass json config filename
                options = JSON.parse(file.readFileSync(opt));
            } else options = opt;
            if (typeof options.recvWindow === "undefined") options.recvWindow = default_options.recvWindow;
            if (typeof options.useServerTime === "undefined") options.useServerTime = default_options.useServerTime;
            if (typeof options.reconnect === "undefined") options.reconnect = default_options.reconnect;
            if (typeof options.test === "undefined") options.test = default_options.test;
            if (typeof options.log === "undefined") options.log = default_options.log;
            if (typeof options.verbose === "undefined") options.verbose = default_options.verbose;

            if (callback) callback();
        },
        websockets: {
            subscribe: function(url, callback, reconnect = false) {
                return subscribe(url, callback, reconnect);
            },
            subscriptions: function() {
                return subscriptions;
            },
            candlesticks: function candlesticks(symbols, interval, callback) {
                let reconnect = function() {
                    if (options.reconnect) candlesticks(symbols, interval, callback);
                };
                // If an array of symbols are sent we use a combined stream connection rather.
                // This is transparent to the developer, and results in a single socket connection.
                // This essentially eliminates "unexpected response" errors when subscribing to a lot of data.
                let subscription = undefined;
                if (Array.isArray(symbols)) {
                    if (!isArrayUnique(symbols))
                        throw Error('candlesticks: "symbols" cannot contain duplicate elements.');
                    let streams = symbols.map(function(symbol) {
                        return `ok_sub_spot_${symbol}_kline_${interval}`;
                    });
                    subscription = subscribeCombined(streams, callback, reconnect, addChannel);
                } else {
                    let symbol = symbols.toLowerCase();
                    subscription = subscribe(
                        `ok_sub_spot_${symbol}_kline_${interval}`,
                        callback,
                        reconnect,
                        addChannel
                    );
                }
                return subscription.endpoint;
            }
        }
    };
})();
