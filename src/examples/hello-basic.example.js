const redis = require('redis');
const logger = require('../utils/logger.util');

// * 1) Create a client and connect to Redis
const client = redis.createClient({
  host: 'localhost',
  port: 6379,
  // password: 'password',
});

// * 2) Set key 'hello' to value 'world'
client.set('hello', 'world', async (_err, reply) => {
  logger.info(reply); // expected output: OK

  // * 3) Get value of key 'hello'
  await client.get('hello', async (_getErr, getReply) => {
    logger.info(getReply); // expected output: world

    // * 4) Close Redis connection and free up resources
    await client.quit();
  });
});
