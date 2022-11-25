import { Kafka, logLevel } from "kafkajs";
import express from "express";
import http from "http";
import { Server } from "socket.io";
import cors from "cors";
import * as dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(cors);

const server = http.createServer(app);
const socket = new Server(server, {
  cors: {
    origin: process.env.ORIGIN,
    methods: ["GET", "POST"],
  },
});

const kafka = new Kafka({
  brokers: [process.env.BROKERS],
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: process.env.USERNAME,
    password: process.env.PASSWORD,
  },
  logLevel: logLevel.NOTHING,
});

const consumer = kafka.consumer({ groupId: "coordinates" });

socket.on("connect", (socket) => {
  console.log("user connected: ", socket.id);
});

server.listen(process.env.PORT, () => {
  console.log("SERVER RUNNING ON PORT ", process.env.PORT);
});

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "coordinates", fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("received: ", message?.value?.toString());
      socket.emit("coordinates", message?.value?.toString());
    },
  });
};

run().catch(console.error);
