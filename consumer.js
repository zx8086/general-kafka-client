"use strict";

const dotenv = require("dotenv");
dotenv.config();

const kafkaInst = require("./kafka");
const consumer = kafkaInst.consumer({ groupId: process.env.GROUP_ID })

const consumeMessages = async () => {
    await consumer.connect();
    const describeGroup  = await consumer.describeGroup()
    console.log(describeGroup);
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
          headers: message.headers,
          key: message.key.toString(),
          value: message.value.toString(),
        })
      },
    });
   };

consumeMessages().catch(e => console.error(`[ ${process.env.TOPIC}/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.forEach(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.forEach(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})

  //  .catch(async (error) => {
  //  console.error(error);
  //  try {
  //  } catch (e) {
  //      console.error("Failed to gracefully disconnect consumer", e);
  //  }
  //  process.exit(1);
  //  });

// "The only sin is to make a choice without knowing you are making one." - Jonathan Shewchuk 
