"use strict";

const dotenv = require("dotenv");
dotenv.config();

const kafkaInst = require("./kafka");

const consumeMessages = async () => {
    const consumer = kafkaInst.consumer({ groupId: process.env.GROUP_ID })
    await consumer.connect();
    await consumer.subscribe({
      topic: process.env.TOPIC,
      fromBeginning: false,
    });    
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log({
          value: message.value.toString(),
        })
      },
    });
   }

consumeMessages()
    .catch(async (error) => {
    console.error(error);
    try {
    } catch (e) {
        console.error("Failed to gracefully disconnect consumer", e);
        await consumer.disconnect();

    }
    process.exit(1);
    });

// "Beware of bugs in the above code; I have only proved it correct, not tried it." - Donald Knuth