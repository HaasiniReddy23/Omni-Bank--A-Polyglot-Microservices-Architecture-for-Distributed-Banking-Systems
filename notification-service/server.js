const express = require("express");
const cors = require("cors");
const amqp = require("amqplib");
const { MongoClient } = require("mongodb");

const app = express();

app.use(cors());
app.use(express.json());

const PORT = 8082;

const RABBITMQ_HOST = process.env.RABBITMQ_HOST || "localhost";
const MONGO_URL = process.env.MONGO_URL || "mongodb://localhost:27017";
const MONGO_DB = process.env.MONGO_DB || "omnibank";

const EXCHANGE = "transfer-events";
const QUEUE = "notification-events";

let notifications;

async function connectMongo() {
    const client = new MongoClient(MONGO_URL);

    await client.connect();

    const db = client.db(MONGO_DB);
    notifications = db.collection("notifications");

    await notifications.createIndex({ createdAt: -1 });

    console.log(">>> MongoDB connected");
}

async function connectRabbitMQ() {
    const connection = await amqp.connect(
        `amqp://${RABBITMQ_HOST}:5672`
    );

    const channel = await connection.createChannel();

    await channel.assertExchange(EXCHANGE, "fanout", {
        durable: true
    });

    await channel.assertQueue(QUEUE, {
        durable: true
    });

    await channel.bindQueue(QUEUE, EXCHANGE, "");

    console.log(">>> RabbitMQ connected");
    console.log(`>>> Listening on ${QUEUE}`);

    channel.consume(QUEUE, async (message) => {
        if (!message) return;

        try {
            const event = JSON.parse(
                message.content.toString()
            );

            console.log(
                ">>> NOTIFICATION RECEIVED:",
                event
            );

            await notifications.insertOne({
                ...event,
                createdAt: new Date()
            });

            console.log(
                ">>> Notification saved to MongoDB"
            );

            channel.ack(message);

        } catch (error) {
            console.error(
                ">>> Notification error:",
                error.message
            );

            channel.nack(message, false, false);
        }
    });
}

app.get("/health", (req, res) => {
    res.json({
        status: "UP",
        service: "OmniBank Notification Service"
    });
});

app.get("/notifications", async (req, res) => {
    try {
        const rows = await notifications
            .find({})
            .sort({ createdAt: -1 })
            .limit(100)
            .toArray();

        res.json({
            notifications: rows
        });

    } catch (error) {
        res.status(500).json({
            error: error.message
        });
    }
});

async function start() {
    try {
        await connectMongo();
        await connectRabbitMQ();

        app.listen(PORT, "0.0.0.0", () => {
            console.log(
                `>>> Notification Service running on port ${PORT}`
            );
        });

    } catch (error) {
        console.error(
            ">>> Startup failed:",
            error
        );

        process.exit(1);
    }
}

start();