// index.js
require('dotenv').config();
const express = require('express');
const { Queue, Worker } = require('bullmq');

// 1) Create an Express app
const app = express();
app.use(express.json());

// 2) Configure Redis connection (BullMQ style)
const connection = {
  host: process.env.REDIS_HOST || 'localhost',
  port: Number(process.env.REDIS_PORT) || 6379,
  username: process.env.REDIS_USERNAME, // if needed for Redis 6+ ACL
  password: process.env.REDIS_PASSWORD, // if needed
  // tls: {} // if your Redis requires TLS/SSL
};

// 3) Create a Queue instance (Producer uses this to add jobs)
const queueName = 'messageQueue';
const messageQueue = new Queue(queueName, { connection });

// 4) Create a Worker (Consumer processes jobs)
const worker = new Worker(
  queueName,
  async (job) => {
    // Job data is whatever was passed to queue.add(...)
    console.log('Processing message:', job.data);

    // Simulate some processing time
    await new Promise((resolve) => setTimeout(resolve, 1000));

    const { text, timestamp } = job.data;
    const processingTime = Date.now() - timestamp;

    console.log(`Message processed in ${processingTime}ms: ${text}`);

    // The return value here is stored as job.returnvalue
    return { processed: true, processingTime };
  },
  {
    concurrency: 2,
    connection
  }
);

// 5) Listen for worker events (optional, for logging/debugging)
worker.on('completed', (job) => {
  console.log(`Job ${job.id} completed successfully!`);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed with error: ${err.message}`);
});

// 6) Define an endpoint for adding messages to the queue
app.post('/message', async (req, res) => {
  try {
    const { message } = req.body;
    if (!message) {
      return res.status(400).json({ status: 'error', message: 'No message provided' });
    }

    // Add a job to the queue
    await messageQueue.add('new-message', {
      text: message,
      timestamp: Date.now()
    });

    return res.json({ status: 'success', message: 'Message added to queue' });
  } catch (error) {
    console.error('Error adding message to queue:', error);
    return res.status(500).json({ status: 'error', message: error.message });
  }
});

// 7) Start the HTTP server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server (producer + consumer) is running on port ${PORT}`);
});
