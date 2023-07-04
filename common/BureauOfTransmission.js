/*
Bureau of Transmission 通政司
============================

This is a small but important service under Cabinet. Utilizing ZeroMQ, it
sets up requesting and responding message queues for each microservice
(Ministries as in our Kingdom of Honneamano).
*/

import _ from "lodash";
import zeromq from "zeromq";


class BureauOfTransmission {

    #requestSock;
    #responseSock;
    #url;

    constructor({ config }){
        this.#url = _.get(config, "url");

        this.#requestSock = new zeromq.Request();
        this.#responseSock = new zeromq.Reply();
    }

    async ready(){
        await this.#requestSock.bind(this.#url);
        await this.#responseSock.bind(this.#url);
        return this;
    }

    async run(){
        for await (const [msg] of this.#responseSock) {
            console.log('Received ' + ': [' + msg.toString() + ']');
            await sock.send('World');
            // Do some 'work'
        }
    }

    async call(method, args){
        return await this.#requestSock.send({
            type: 'call',
            method,
            args,
        });
    }

    register(method, handler){
        return this;
    }

}

var agentOfTransmission = null;

export default init_or_get(config){
    if(_.isNil(agentOfTransmission) && !config){
        throw Error("Missing config for BureauOfTransmission.");
    }
    if(!agentOfTransmission){
        agentOfTransmission = new BureauOfTransmission(config);
        return agentOfTransmission.ready().run();
    }
    return agentOfTransmission;
}