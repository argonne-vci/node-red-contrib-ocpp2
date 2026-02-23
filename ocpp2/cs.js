/* CS: Cherging Station

  Title: Node-Red-Contrib-OCPP2
  Author: Bryan Nystrom
  Company: Argonne National Laborator

  File: cs.js
  Description: CS (Charge Station) node.

*/

"use strict";

const Websocket = require("ws");

// This will validate incoming and outgoing ocpp2.0.1 messages
// for proper format
const Validator = require("jsonschema").Validator;
const schema_val = new Validator();
// we need this for reading the validation schema
const fs = require("fs");

const path = require("node:path");

//Use nodejs built-in crypto for uuid
const crypto = require("crypto");
const { clearTimeout } = require("timers");

//const Logger = require('./utils/logdata');
const debug = require("debug")("anl:ocpp2:cs");

// Array position in OCPP message
const MSGTYPE = 0;
const MSGID = 1;
const MSGACTION = 2;
const MSGREQPAYLOAD = 3;
const MSGRESPAYLOAD = 2;

// Types of messages
const REQUEST = 2;
const RESPONSE = 3;
const CALLERROR = 4;
const CONTROL = 99;

// Retry Backoff defaults
const WSRTMIN_DEF = 1;
const WSRTRPT_DEF = 2;
const WSRTRND_DEF = 0;

const OCPPPROTOCOL = ["ocpp2.0.1"];

let NetStatus = "BOOT";

const cmdIdMap = new Map();

module.exports = function (RED) {
  function OcppChargeStationNode(config) {
    RED.nodes.createNode(this, config);
    let hPingTimer = null;

    // Copy in the config values
    //
    this.cbId = config.cbId;
    this.csms = RED.nodes.getNode(config.csms);
    this.csms_url = this.csms.url;
    this.csms_user = this.cbId;
    this.csms_pw = this.credentials.csms_pw;
    this.messageTimeout = config.messageTimeout || 10000;
    this.auto_connect = config.auto_connect;
    this.logging_enabled = config.ocpp_logging;

    this.ws_rt_minimum = isNaN(Number.parseInt(config.ws_rt_minimum))
      ? WSRTMIN_DEF
      : Number.parseInt(config.ws_rt_minimum);
    this.ws_rt_repeat = isNaN(Number.parseInt(config.ws_rt_repeat))
      ? WSRTRPT_DEF
      : Number.parseInt(config.ws_rt_repeat);
    this.ws_rt_rnd_range = isNaN(Number.parseInt(config.ws_rt_rnd_range))
      ? WSRTRNG_DEF
      : Number.parseInt(config.ws_rt_rnd_range);

    this.NetStatus = NetStatus;
    this.debug = debug;

    const node = this;

    node.status({ fill: "blue", shape: "ring", text: "OCPP CS 2.0.1" });

    let csmsURL;
    let ws;
    let wsreconncnt = 0;
    let wstocur = parseInt(node.ws_rt_minimum);
    let conto = null;
    let wstryreconn = true;

    try {
      csmsURL = new URL(node.csms_url);
    } catch (error) {
      node.status({ fill: "red", shape: "ring", text: error });
      debug(`URL error: ${error}`);
      return;
    }

    csmsURL.protocol = "ws";
    csmsURL.username = node.csms_user;
    csmsURL.password = node.csms_pw;
    csmsURL.pathname = path.join(csmsURL.pathname, node.cbId);

    const ws_options = {
      handshaketimeout: 5000,
      connectTimeout: 5000,
    };

    function log_ocpp_msg(ocpp_msg, msgFrom) {
      if (node.logging_enabled) {
        debug(`node.logging_enabled = ${node.logging_enabled}`);
        let msg = {
          timestamp: Date.now(),
          msgFrom,
          payload: ocpp_msg,
        };
        node.send([null, null, msg]);
      }
    }

    function reconn_debug() {
      debug(`ws_rt_minimum: ${node.ws_rt_minimum}`);
      debug(`ws_rt_repeat: ${node.ws_rt_repeat}`);
      debug(`ws_rt_rnd_range: ${node.ws_rt_rnd_range}`);
      debug(`wstocur: ${wstocur}`);
      debug(`wsreconncnt: ${wsreconncnt}`);
    }

    let ws_open = function () {
      let msg = {};
      msg.ocpp = {};
      wsreconncnt = 0;
      wstocur = parseInt(node.ws_rt_minimum);
      node.status({ fill: "green", shape: "dot", text: "Connected..." });
      node.wsconnected = true;
      wsreconncnt = 0;
      msg.ocpp.websocket = "ONLINE";
      if (node.NetStatus != msg.ocpp.websocket) {
        node.send([msg, null, null]); //send update
        node.NetStatus = msg.ocpp.websocket;
      }

      // Add a ping intervale timer
      hPingTimer = setInterval(() => {
        try {
          if (ws) {
            ws.ping();
          }
        } catch (error) {
          debug(`Ping Error: ${error}`);
        }
      }, 30000);
    };

    let ws_close = function (code, reason) {
      let msg = {};
      msg.ocpp = {};
      debug(`Websocket closed code: ${code.code}`);
      // debug(`Websocket closed reason: ${reason}`);
      node.status({ fill: "red", shape: "dot", text: "Closed" });
      node.wsconnected = false;
      msg.ocpp.websocket = "OFFLINE";
      debug("WENT TO OFFLINE....");
      if (node.NetStatus != msg.ocpp.websocket) {
        debug("SENDING MSG WITH WEBSOCKET OFFLINE");
        node.send([msg, null, null]); //send update
        node.NetStatus = msg.ocpp.websocket;
      }
      // Stop the ping timer
      if (hPingTimer != null) {
        clearInterval(hPingTimer);
        hPingTimer = null;
      }

      // remove all the events from the ws object
      if (ws) {
        ws.removeEventListener("open", ws_open);
        ws.removeEventListener("close", ws_close);
        ws.removeEventListener("error", ws_error);
        ws.removeEventListener("message", ws_message);
      }

      if (wstryreconn == true) {
        // Inc the reconnection try count
        wsreconncnt += 1;
        node.status({
          fill: "red",
          shape: "ring",
          text: `Reconn #${wsreconncnt} in ${wstocur}sec `,
        });
        conto = setTimeout(
          () => ws_connect(),
          wstocur * 1000 +
            (Math.random() * (node.ws_rt_rnd_range * 1000 - 0) + 0),
        );
        debug(`reconnect timeout: ${wstocur}`);
        debug(`retry minimum: ${node.ws_rt_minimum}`);
        debug(`reconnect cnt: ${wsreconncnt}`);
        // adjust the timeout value for the next round
        if (wsreconncnt <= node.ws_rt_repeat) {
          wstocur += node.ws_rt_minimum;
        }
      } else {
        node.status({ fill: "red", shape: "dot", text: `Closed` });
      }
    };

    let ws_error = function (err) {
      node.log("Websocket error:", { err });
      // debug('Websocket error:', {err});
    };

    let ws_message = function (event) {
      let msgIn = event.data;
      let cbId = node.cbId;
      debug(`Got a message from CSMS: ${msgIn}`);
      // let ocpp2 = JSON.parse(msgIn);
      //
      let ocpp2;

      //////////////////////////////
      // This should never happen //
      // ..but I've seen EVSEs    //
      // do it..                  //
      /////////////////////////////
      if (msgIn[0] != "[") {
        ocpp2 = JSON.parse("[" + msgIn + "]");
      } else {
        ocpp2 = JSON.parse(msgIn);
      }
      log_ocpp_msg(ocpp2, "CSMS");

      let msgTypeStr = ["Request", "Response", "Error"][ocpp2[MSGTYPE] - 2];

      // REQUEST, RESPONSE, or ERROR?
      //
      if (ocpp2[MSGTYPE] == REQUEST || ocpp2[MSGTYPE] == RESPONSE) {
        let msg = {};
        msg.ocpp = {};
        msg.payload = {};
        let id;
        let mapObj = {};
        msg.ocpp.ocppVersion = "2.0.1";

        switch (ocpp2[MSGTYPE]) {
          case REQUEST:
            msg.payload.data = ocpp2[MSGREQPAYLOAD] || {};
            msg.payload.command = ocpp2[MSGACTION] || null;
            msg.payload.MessageId = ocpp2[MSGID];
            msg.payload.cbId = cbId;
            msg.ocpp.MessageId = ocpp2[MSGID];
            id = ocpp2[MSGID];
            mapObj = {
              cbId: msg.payload.cbId,
              command: ocpp2[MSGACTION],
              time: new Date(),
            };

            cmdIdMap.set(id, mapObj);

            setTimeout(
              function () {
                if (cmdIdMap.has(id)) {
                  let expCmd = cmdIdMap.get(id);
                  debug(
                    `Expired Req: id: ${id}, cbId: ${expCmd.cbId} cmd: ${expCmd.command}`,
                  );
                  cmdIdMap.delete(id);
                }
              },
              node.messageTimeout,
              id,
            );
            break;
          case RESPONSE:
            msg.payload.data = ocpp2[MSGRESPAYLOAD] || {};
            if (cmdIdMap.has(ocpp2[MSGID])) {
              let c = cmdIdMap.get(ocpp2[MSGID]);

              msg.payload.command = c.command;

              if (Object.prototype.hasOwnProperty.call(c, "customData")) {
                msg.customData = c.customData;
              }
              if (Object.prototype.hasOwnProperty.call(c, "_linkSource")) {
                msg._linkSource = c._linkSource;
              }
              cmdIdMap.delete(ocpp2[MSGID]);
            } else {
              let errMsg = `Expired or invalid RESPONSE: ${ocpp2[MSGID]}`;
              debug(errMsg);
              node.error(errMsg);
              return;
            }
            break;
        }

        msg.topic = `${cbId}/${msgTypeStr}`;
        //debug(msg.topic);

        msg.ocpp.command = msg.payload.command;
        msg.ocpp.cbId = cbId;

        // Check valididty of the command schema
        //
        let schemaName = `${msg.payload.command}${msgTypeStr}.json`;

        let schemaPath = path.join(__dirname, "schemas", schemaName);

        // By first checking if the file exists, we check that the command is an
        // acutal ocpp2.0.1 command
        if (fs.existsSync(schemaPath)) {
          let schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
          // Remove $id field to avoid URL parsing errors with URN format in newer Node.js versions
          delete schema.$id;

          let val = schema_val.validate(ocpp2[MSGREQPAYLOAD], schema);

          if (val.errors.length > 0) {
            let invalidOcpp2 = val.errors;
            debug("*********INVALID MESSAGE FORMAT************");
            debug(JSON.stringify(ocpp2));

            done(invalidOcpp2);
            node.error({ invalidOcpp2 });
            return;
          }
        } else {
          let errMsg = `Invalid OCPP2.0.1 command: ${ocpp2[MSGACTION]}`;
          node.error(errMsg);
          done(errMsg);
          return;
        }

        Object.prototype.hasOwnProperty.call(msg, "_linkSource")
          ? node.send([null, msg, null])
          : node.send([msg, null, null]);
      }
    };

    function ws_connect() {
      //
      //reconn_debug();
      try {
        debug(`---------------------------------------`);
        debug(`URL: ${csmsURL.href}`);
        ws = new Websocket(csmsURL.href, OCPPPROTOCOL, ws_options);
        ws.timeout = 5000;
        ws.addEventListener("open", ws_open);
        ws.addEventListener("close", ws_close);
        ws.addEventListener("error", ws_error);
        ws.addEventListener("message", ws_message);
      } catch (error) {
        debug(`Websocket Error: ${error}`);
        return;
      }
    }

    function ws_reconnect() {
      debug("Clearing Timeout");
      if (conto) clearTimeout(conto);
      try {
        if (ws) {
          ws.removeEventListener("open", ws_open);
          ws.removeEventListener("close", ws_close);
          ws.removeEventListener("error", ws_error);
          ws.removeEventListener("message", ws_message);
          ws.close();
          //ws = null;
        }
        if (conto) clearTimeout(conto);
        ws_connect();
      } catch (error) {
        debug(`Websocket Error: ${error}`);
        return;
      }
    }

    node.on("close", function (msg, send, done) {
      //ws.close(1000);
      node.status({ fill: orange, shape: "square", text: "Stopping..." });

      if (ws) {
        //clearTimeout(conto);
        ws.close();
        ws = null;
        node.status({ fill: "red", shape: "dot", text: "Closed" });
      }
      done();
    });
    ////////////////////////////////////////////
    // This section is for input from a the   //
    // Node itself                            //
    ////////////////////////////////////////////

    node.on("input", function (msg, send, done) {
      if (msg.payload.data) {
        debug("++++++++++++++++++++++++++++++++++");
        debug("has msg.payload.data");
        debug(JSON.stringify(msg.payload));
        debug("++++++++++++++++++++++++++++++++++");
      }

      let ocpp2 = [];

      ocpp2[MSGTYPE] = msg.payload.msgType || REQUEST;
      ocpp2[MSGID] = msg.payload.MessageId || crypto.randomUUID();

      if (ocpp2[MSGTYPE] == CONTROL) {
        ocpp2[MSGACTION] = msg.payload.command || node.command;

        if (!ocpp2[MSGACTION]) {
          const errStr =
            "ERROR: Missing Control Command in JSON request message";
          node.error(errStr);
          debug(errStr);
          return;
        }

        switch (ocpp2[MSGACTION].toLowerCase()) {
          case "connect":
            if (conto) clearTimeout(conto);
            if (msg.payload.data) {
              debug("++++++++++++++++++++++++++++++++++");
              debug("has msg.payload.data");
              debug("++++++++++++++++++++++++++++++++++");
              if (Object.prototype.hasOwnProperty.call(msg, "cbId")) {
                this.cbId = msg.payload.data.cbId;
              }
              if (
                Object.prototype.hasOwnProperty.call(
                  msg.payload.data,
                  "password",
                )
              ) {
                debug("++++++++++++++++++++++++++++++++++");
                debug("has msg.payload.data.password");
                debug("++++++++++++++++++++++++++++++++++");
                node.csms_pw = msg.payload.data.password;
              }
              if (
                Object.prototype.hasOwnProperty.call(
                  msg.payload.data,
                  "csmsUrl",
                )
              ) {
                this.csms_url = msg.payload.data.csmsUrl.endsWith("/")
                  ? msg.payload.data.csmsUrl.slice(0, -1)
                  : msg.payload.data.csmsUrl;
              }
            }

            try {
              csmsURL.href = node.csms_url;
              csmsURL.protocol = "ws";
              // csmsURL.username = node.csms_user;
              csmsURL.username = node.cbId;
              csmsURL.password = node.csms_pw;
              csmsURL.pathname = path.join(csmsURL.pathname, node.cbId);
              debug(csmsURL.href);
            } catch (error) {
              node.status({ fill: "red", shape: "ring", text: error });
              debug(`URL error: ${error}`);
              return;
            }
            wstryreconn = true;
            ws_reconnect();
            break;
          case "close":
            wstryreconn = false;
            if (conto) clearTimeout(conto);
            if (ws) {
              //clearTimeout(conto);
              ws.close();
              ws = null;
              node.status({ fill: "red", shape: "dot", text: "Closed" });
            }
            break;
          case "retrybackoff":
            if (Object.prototype.hasOwnProperty.call(msg.payload, "data")) {
              let data = msg.payload.data;
              if (
                Object.prototype.hasOwnProperty.call(
                  data,
                  "RetryBackOffWaitMinimum",
                )
              ) {
                node.ws_rt_minimum =
                  parseInt(msg.payload.data.RetryBackOffWaitMinimum) ||
                  node.ws_rt_minimum;
                wstocur = node.ws_rt_minimum;
                wsreconncnt = 0;
              }
              if (
                Object.prototype.hasOwnProperty.call(
                  data,
                  "RetryBackOffRandomRange",
                )
              ) {
                node.ws_rt_rnd_random =
                  parseInt(msg.payload.data.RetryBackOffRandomRange) ||
                  node.ws_rt_rnd_range;
              }
              if (
                Object.prototype.hasOwnProperty.call(
                  data,
                  "RetryBackOffRepeatTime",
                )
              ) {
                node.ws_rt_repeat =
                  ParseInt(msg.payload.data.RetryBackOffRepeatTime) ||
                  node.ws_rt_repeat;
              }
            }
            wsreconncnt = 0;
            reconn_debug();
            break;
          default:
            break;
        }
        //logger.log(msgTypeStr[request[MSGTYPE]], JSON.stringify(request).replace(/,/g, ', '));
      } else if (node.wsconnected == true) {
        if (ocpp2[MSGTYPE] == REQUEST) {
          ocpp2[MSGACTION] = msg.payload.command || null;
          ocpp2[MSGREQPAYLOAD] = msg.payload.data || {};
          ocpp2[MSGID] = msg.payload.MessageId || crypto.randomUUID();

          // Check for missing command in object
          if (!ocpp2[MSGACTION]) {
            const errStr = "ERROR: Missing Command in JSON request message";
            debug(errStr);
            node.error(errStr);
            done(errStr);
            return;
          }

          log_ocpp_msg(ocpp2, "CS");

          // Check valididty of the command schema
          //
          let schemaName = `${ocpp2[MSGACTION]}Request.json`;

          let schemaPath = path.join(__dirname, "schemas", schemaName);

          // By first checking if the file exists, we check that the command is an
          // acutal ocpp2.0.1 command
          //
          if (fs.existsSync(schemaPath)) {
            let schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
            // Remove $id field to avoid URL parsing errors with URN format in newer Node.js versions
            delete schema.$id;

            let val = schema_val.validate(ocpp2[MSGREQPAYLOAD], schema);

            if (val.errors.length > 0) {
              debug("*********INVALID MESSAGE FORMAT************");
              debug(`SchemaName ${schemaName}`);
              debug(JSON.stringify(ocpp2[MSGREQPAYLOAD]));
              debug(`msg.payload.data: ${JSON.stringify(msg.payload.data)}`);
              let invalidOcpp2 = val.errors;
              done(invalidOcpp2);
              node.error({ invalidOcpp2 });
              return;
            }
          } else {
            let errMsg = `Invalid OCPP2.0.1 command: ${ocpp2[MSGACTION]}`;
            debug(errMsg);
            node.error(errMsg);
            done(errMsg);
            return;
          }

          ocpp2[MSGACTION] = msg.payload.command; // || node.command;

          ocpp2[MSGREQPAYLOAD] = msg.payload.data || {}; // cmddata || {};
          if (!ocpp2[MSGREQPAYLOAD]) {
            const errStr = "ERROR: Missing Data in JSON request message";
            node.error(errStr);
            debug(errStr);
            done(errStr);
            return;
          }

          let id = ocpp2[MSGID];
          let c = {
            cbId: msg.payload.cbId,
            command: ocpp2[MSGACTION],
            time: new Date(),
          };

          // Save the return link node path if it exists
          if (Object.prototype.hasOwnProperty.call(msg, "_linkSource")) {
            c._linkSource = JSON.parse(JSON.stringify(msg._linkSource));
          }

          if (Object.prototype.hasOwnProperty.call(msg, "customData")) {
            c.customData = JSON.parse(JSON.stringify(msg.customData));
          }

          cmdIdMap.set(id, c);
          setTimeout(
            function () {
              if (cmdIdMap.has(id)) {
                let expCmd = cmdIdMap.get(id);
                debug(
                  `Expired Req: id: ${id}, cbId: ${expCmd.cbId} cmd: ${expCmd.command}`,
                );
                cmdIdMap.delete(id);
              }
            },
            node.messageTimeout,
            id,
          );

          let ocpp_msg = JSON.stringify(ocpp2);
          debug(`Sending message: ${ocpp_msg}`);
          ws.send(ocpp_msg);

          node.status({
            fill: "green",
            shape: "dot",
            text: `REQ out: ${ocpp2[MSGACTION]}`,
          });
        } else {
          // Assuming the call is a RESPONSE to an existing REQUEST
          ocpp2[MSGRESPAYLOAD] = msg.payload.data || {};
          ocpp2[MSGID] = msg.ocpp.MessageId;

          log_ocpp_msg(ocpp2, "CS");

          if (cmdIdMap.has(ocpp2[MSGID])) {
            // Check valididty of the command schema
            //
            let MSGACTION = cmdIdMap.get(ocpp2[MSGID]).command;
            let schemaName = `${MSGACTION}Response.json`;

            let schemaPath = path.join(__dirname, "schemas", schemaName);

            // By first checking if the file exists, we check that the command is an
            // acutal ocpp2.0.1 command
            if (fs.existsSync(schemaPath)) {
              let schema = JSON.parse(fs.readFileSync(schemaPath, "utf8"));
              // Remove $id field to avoid URL parsing errors with URN format in newer Node.js versions
              delete schema.$id;

              let val = schema_val.validate(ocpp2[MSGRESPAYLOAD], schema);

              if (val.errors.length > 0) {
                //val.errors.forEach( (x) => { node.error({x}) });
                let invalidOcpp2 = val.errors;
                debug("*********INVALID MESSAGE FORMAT************");
                debug(JSON.stringify(ocpp2));
                done(invalidOcpp2);
                node.error({ invalidOcpp2 });
                return;
              }
            } else {
              let errMsg = `Invalid OCPP2.0.1 command: ${MSGACTION}`;
              node.error(errMsg);
              done(errMsg);
              return;
            }
          } else {
            let msgError = `Target message Id is missing or expired: ${ocpp2[MSGID]}`;
            node.error(msgError);
            done(msgError);
            return;
          }

          cmdIdMap.delete(ocpp2[MSGID]);
          let ocpp_msg = JSON.stringify(ocpp2);
          debug(`Sending message: ${ocpp_msg}`);
          ws.send(ocpp_msg, ocpp_msg);

          node.status({
            fill: "green",
            shape: "dot",
            text: `RES out: ${MSGACTION}`,
          });
        }
      }
    });

    // Only do this if auto-connect is enabled
    //
    if (node.auto_connect && csmsURL) {
      node.status({ fill: "blue", shape: "dot", text: `Connecting...` });
      ws_connect();
    }
  }

  RED.nodes.registerType("CS", OcppChargeStationNode, {
    credentials: {
      csms_pw: { type: "password" },
    },
  });

  /***********************************************************************
   * THis sets up the configuration node for target CSMSs
   *
   ***********************************************************************/

  function CSMSConfigNode(n) {
    RED.nodes.createNode(this, n);
    this.url = n.url;
    this.name = n.name || n.url;
  }

  RED.nodes.registerType("target-csms", CSMSConfigNode);
};
