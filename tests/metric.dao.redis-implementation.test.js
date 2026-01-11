const config = require('better-config');
const redis = require('../src/daos/impl/redis/redis-client');
const redisMetricDAO = require('../src/daos/impl/redis/metric.dao.redis-implementation');
const keyGenerator = require('../src/daos/impl/redis/redis-key-generator');
const timeUtils = require('../src/utils/time.util');
config.set('../config.json');

const testSuiteName = 'metric.dao.redis-implementation.test';
const testKeyPrefix = `test:${testSuiteName}`;

keyGenerator.setPrefix(testKeyPrefix);

const client = redis.getClient();
const sampleReadings = [];

beforeAll(() => {
  jest.setTimeout(60000);

  // Create the sample data.
  let time = timeUtils.getCurrentTimestamp();

  for (let n = 0; n < 72 * 60; n += 1) {
    const reading = {
      siteId: 1,
      whUsed: n,
      whGenerated: n,
      tempC: n,
      dateTime: time,
    };

    sampleReadings.push(reading);

    // Set time to one minute earlier.
    time -= 60;
  }
});

afterEach(async () => {
  const testKeys = await client.keysAsync(`${testKeyPrefix}:*`);

  if (testKeys.length > 0) {
    await client.delAsync(testKeys);
  }
});

afterAll(() => {
  client.quit();
});

// Inserts then retrieves up to limit metrics.
const testInsertAndRetrieve = async (limit) => {
  for (const reading of sampleReadings) {
    await redisMetricDAO.insert(reading);
  }

  // Retrieve up to 'limit' metrics back.
  const measurements = await redisMetricDAO.getRecent(1, 'whGenerated', timeUtils.getCurrentTimestamp(), limit);

  // Make sure we got the right number back.
  expect(measurements.length).toEqual(limit);

  let n = limit;

  for (const measurement of measurements) {
    expect(measurement.value).toEqual(n - 1);
    n -= 1;
  }
};

// * These tests are for Challenge #2
test(`${testSuiteName}: test 1 reading`, async () => testInsertAndRetrieve(1));

test(`${testSuiteName}: test 1 day of readings`, async () => testInsertAndRetrieve(60 * 24));

test(`${testSuiteName}: test multiple days of readings`, async () => testInsertAndRetrieve(60 * 70));
