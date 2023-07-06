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
import { v4 as uuidv4 } from "uuid";


const RPC_TIMEOUT = 5000;
const RESERVED_TYPES = ["error", "request", "response"];


class Envelope {

    constructor(serializedBase64){
        if(_.isNil(serializedBase64)){
            this.uuid = uuidv4();
            return;
        }

        try{
            let data = unpack(Buffer.from(serializedBase64, "base64"));
            let [ type, sender, receiver, payload, uuid ] = data;

            if(![ type, sender, receiver, uuid].every(e=>_.isString(e))){
                throw Error();
            }

            this.sender     = sender;
            this.receiver   = receiver;
            this.payload    = payload;
            this.type       = type.toLowerCase();
            this.uuid       = uuid;
        } catch(e){
            throw Error("Error parsing enveloped data.");
        }
    }

    toString(){
        return Buffer.from(pack(
            [this.type, this.sender, this.receiver, this.payload, this.uuid]
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

    #responderFunctions;
    #pendingRequests;
    
    constructor(args){
        super();

        this.#client = redis.createClient(
            _.get(args, "config.port"),
            _.get(args, "config.host")
        );

        this.#pendingRequests = new Map();

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



    async request(targetName, args, options){
        const newUUID = await this.sendMessage(
            "request",
            targetName,
            args
        );
        const timeout = _.get(options, "timeout", RPC_TIMEOUT);
        return new Promise((resolve, reject)=>{
            this.#pendingRequests.set(
                newUUID,
                (...args)=>{
                    resolve.call(this, ...args);
                    this.#pendingRequests.delete(newUUID);
                }
            );
            setTimeout(
                ()=>{
                    this.#pendingRequests.delete(newUUID);
                    reject(new Error("Timeout."));
                },
                timeout
            );
        });
    }

    #onRequest(envelope){
        // Emit the "request" event, call other code to handle. 
        this.emit("request", envelope.payload, (result)=>{
            this.sendMessage(
                "response", 
                envelope.sender,
                [envelope.uuid, result]
            );
        });
    }

    #onResponse(envelope){
        let originalUUID = _.get(envelope, "payload.0"),
            result       = _.get(envelope, "payload.1");
        let resolveFunc = this.#pendingRequests.get(originalUUID);
        if(!_.isFunction(resolveFunc)) return;
        resolveFunc(result);
    }



    #onMessage(encodedPayload, channel){
        try{
            const data = new Envelope(encodedPayload);
            
            if(data.sender == this.#localName) return; // ignore self messages

            switch(data.type){
            case "request":
                this.#onRequest(data);
                break;
            case "response":
                this.#onResponse(data);
                break;
            default:
                // filtered out reserved types and emit
                if(_.includes(RESERVED_TYPES, data.type)) return; // invalid
                this.emit(data.type, data);
            }
        } catch(e){
            console.error(e);
        }
    }

    async sendMessage(type, targetName, payload){
        const data       = new Envelope();
        data.sender      = this.#localName;
        data.receiver    = targetName;
        data.payload     = payload;
        data.type        = type;

        await this.#client.publish(
            data.receiverAddress,
            data.toString()
        );

        return data.uuid;
    } 



}



export { AgentOfTransmission, Envelope };