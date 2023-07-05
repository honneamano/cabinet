/*
Bureau of Transmission 通政司
============================

This is a small but important service under Cabinet. Utilizing ZeroMQ, it
sets up requesting and responding message queues for each microservice
(Ministries as in our Kingdom of Honneamano).
*/

import events from "events";
import _ from "lodash";
import * as redis from "redis";
import { pack, unpack } from "msgpackr";
import { Buffer } from "buffer";


class Envelope {

    constructor(serializedBase64){
        if(_.isNil(serializedBase64)) return;

        try{
            let data = unpack(Buffer.from(serializedBase64, "base64"));
            let [ type, sender, receiver, payload ] = data;

            if(![ type, sender,receiver].every(e=>_.isString(e))){
                throw Error();
            }

            this.sender     = sender;
            this.receiver   = receiver;
            this.payload    = payload;
            this.type       = type.toLowerCase();
        } catch(e){
            throw Error("Error parsing enveloped data.");
        }
    }

    toString(){
        return Buffer.from(pack(
            [this.type, this.sender, this.receiver, this.payload]
        )).toString('base64');
    }

    get senderAddress(){
        return ["honneamano", this.sender, this.type].join(":");
    }

    get receiverAddress(){
        return ["honneamano", this.receiver, this.type].join(":");
    }

}





class AgentOfTransmission extends events.EventEmitter {

    #client;
    #localName;

    #subscriber;
    #publisher;
    
    constructor(args){
        super();

        this.#client = redis.createClient(
            _.get(args, "config.port"),
            _.get(args, "config.host")
        );

        this.#localName = _.get(args, "localName");

        this.#subscriber = this.#client.duplicate();
        
    }

    async ready(){
        await this.#client.connect();
        await this.#subscriber.connect();
        this.#subscriber.pSubscribe(
            "honneamano:" + this.#localName + ":*",
            (a,b)=>this.#onMessage(a,b)
        );
        return this;
    }

    #onMessage(encodedPayload, channel){
        try{
            const data = new Envelope(encodedPayload);
            const reservedTypes = ["error"];

            if(data.sender == this.#localName) return; // ignore self messages
            if(_.includes(reservedTypes, data.type)) return; // invalid

            this.emit(data.type, data);
        } catch(e){
            console.error(e);
        }
    }



    sendMessage(type, targetName, payload){
        const data       = new Envelope();
        data.sender      = this.#localName;
        data.receiver    = targetName;
        data.payload     = payload;
        data.type        = type;

        return this.#client.publish(
            data.receiverAddress,
            data.toString()
        );
    } 



}



export { AgentOfTransmission, Envelope };