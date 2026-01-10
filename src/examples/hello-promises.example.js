const redis = require('redis');
const { promisify } = require('util');

// * 1) Create a client and connect to Redis
const client = redis.createClient({
  host: 'localhost',
  port: 6379,
  // password: 'password',
});

// * 2) Promisify Redis commands we plan to use
const setAsync = promisify(client.set).bind(client);
const getAsync = promisify(client.get).bind(client);

// * 3) Chain promises together to call Redis commands and process the results.
setAsync('hello', 'world')
  .then((res) => console.log(res)) // OK
  .then(() => getAsync('hello'))
  .then((res) => console.log(res)) // world
  .then(() => client.quit());
