"use strict";

const dotenv = require("dotenv");
const { json } = require("express");
dotenv.config();

const kafkaInst = require("./kafka");

const consumeMessages = async () => {
    const consumer = kafkaInst.consumer({ groupId: process.env.GROUP_ID })
    await consumer.connect();
    const data = await consumer.describeGroup()
    console.log(data);
    await consumer.subscribe({
      topic: process.env.TOPIC,
      fromBeginning: true,
    });    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          topic: topic,
          partition: partition,
          offset: message.offset,
          key: message.key.toString(),
          headers: message.headers,
          value: message.value.toString(),
        })
      },
    });
   };

consumeMessages()
    .catch(async (error) => {
    console.error(error);
    try {
    } catch (e) {
        console.error("Failed to gracefully disconnect consumer", e);
    }
    process.exit(1);
    });

// "The only sin is to make a choice without knowing you are making one." - Jonathan Shewchuk 