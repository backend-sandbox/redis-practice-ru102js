const redis = require('redis');
const bluebird = require('bluebird');
const logger = require('../utils/logger.util');

/**
 * Make all functions in 'redis' available as promisified
 * versions whose names end in 'Async'.
 */
bluebird.promisifyAll(redis);

async function runApplication() {
  try {
    // * 1) Create a client and connect to Redis
    const client = redis.createClient({
      host: 'localhost',
      port: 6379,
      // password: 'password',
    });

    const reply = await client.setAsync('hello', 'world');
    logger.info(reply); // expected output: OK

    const keyValue = await client.getAsync('hello');
    logger.info(keyValue); // expected output: world

    client.quit();
  } catch (error) {
    logger.info(error);
  }
}

runApplication();
