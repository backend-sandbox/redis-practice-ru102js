const redis = require('./redis-client');
const keyGenerator = require('./redis-key-generator');

// Minimum amount of capacity that a site should have to be
// considered as having 'excess capacity'.
const capacityThreshold = 0.2;

/**
 * Takes a flat key/value pairs object representing a Redis hash, and
 * returns a new object whose structure matches that of the site domain
 * object.  Also converts fields whose values are numbers back to
 * numbers as Redis stores all hash key values as strings.
 *
 * @param {Object} siteHash - object containing hash values from Redis
 * @returns {Object} - object containing the values from Redis remapped
 *  to the shape of a site domain object.
 * @private
 */
const remap = (siteHash) => {
  const remappedSiteHash = { ...siteHash };

  remappedSiteHash.id = parseInt(siteHash.id, 10);
  remappedSiteHash.panels = parseInt(siteHash.panels, 10);
  remappedSiteHash.capacity = parseFloat(siteHash.capacity, 10);

  // coordinate is optional.
  if (siteHash.hasOwnProperty('lat') && siteHash.hasOwnProperty('lng')) {
    remappedSiteHash.coordinate = {
      lat: parseFloat(siteHash.lat),
      lng: parseFloat(siteHash.lng),
    };

    // Remove original fields from resulting object.
    delete remappedSiteHash.lat;
    delete remappedSiteHash.lng;
  }

  return remappedSiteHash;
};

/**
 // ! STORES SITE GEOLOCATION DATA
 * 
 * Takes a site domain object and flattens its structure out into
 * a set of key/value pairs suitable for storage in a Redis hash.
 *
 * @param {Object} site - a site domain object.
 * @returns {Object} - a flattened version of 'site', with no nested
 *  inner objects, suitable for storage in a Redis hash.
 * @private
 */
function flatten(site) {
  const flattenedSite = { ...site };

  if (flattenedSite.hasOwnProperty('coordinate')) {
    flattenedSite.lat = flattenedSite.coordinate.lat;
    flattenedSite.lng = flattenedSite.coordinate.lng;
    delete flattenedSite.coordinate;
  }

  return flattenedSite;
}

/**
 * Insert a new site.
 *
 * @param {Object} site - a site object.
 * @returns {Promise} - a Promise, resolving to the string value
 *   for the key of the site Redis.
 */
const insert = async (site) => {
  const client = redis.getClient();

  const siteHashKey = keyGenerator.getSiteHashKey(site.id);

  await client.hmsetAsync(siteHashKey, flatten(site));

  // Co-ordinates are required when using this version of the DAO.
  if (!site.hasOwnProperty('coordinate')) {
    throw new Error('Coordinate required for site geo insert!');
  }

  await client.geoaddAsync(keyGenerator.getSiteGeoKey(), site.coordinate.lng, site.coordinate.lat, site.id);

  return siteHashKey;
};

/**
 * Get the site object for a given site ID.
 *
 * @param {number} id - a site ID.
 * @returns {Promise} - a Promise, resolving to a site object.
 */
const findById = async (id) => {
  const client = redis.getClient();
  const siteKey = keyGenerator.getSiteHashKey(id);

  const siteHash = await client.hgetallAsync(siteKey);

  return siteHash === null ? siteHash : remap(siteHash);
};

/**
 * Get an array of all site objects.
 *
 * @returns {Promise} - a Promise, resolving to an array of site objects.
 */
const findAll = async () => {
  const client = redis.getClient();

  const siteIds = await client.zrangeAsync(keyGenerator.getSiteGeoKey(), 0, -1);

  // Create a pipeline to batch all HGETALL commands
  const pipeline = client.batch();

  // Queue all HGETALL commands in the pipeline
  for (const siteId of siteIds) {
    const siteKey = keyGenerator.getSiteHashKey(siteId);
    pipeline.hgetall(siteKey);
  }

  // Execute all commands in a single round trip to Redis
  const results = await pipeline.execAsync();

  // Process the results from the pipeline
  const sites = [];
  for (const siteHash of results) {
    if (siteHash) {
      // Call remap to remap the flat key/value representation
      // from the Redis hash into the site domain object format.
      sites.push(remap(siteHash));
    }
  }

  return sites;
};

/**
 * Get an array of sites within a radius of a given coordinate.
 *
 * @param {number} lat - Latitude of the coordinate to search from.
 * @param {number} lng - Longitude of the coordinate to search from.
 * @param {number} radius - Radius in which to search.
 * @param {'KM' | 'MI'} radiusUnit - The unit that the value of radius is in.
 * @returns {Promise} - a Promise, resolving to an array of site objects.
 */
const findByGeo = async (lat, lng, radius, radiusUnit) => {
  const client = redis.getClient();

  const siteIds = await client.georadiusAsync(keyGenerator.getSiteGeoKey(), lng, lat, radius, radiusUnit.toLowerCase());

  const sites = [];

  for (const siteId of siteIds) {
    const siteKey = keyGenerator.getSiteHashKey(siteId);
    const siteHash = await client.hgetallAsync(siteKey);

    if (siteHash) {
      sites.push(remap(siteHash));
    }
  }

  return sites;
};

/**
 * Get an array of sites where capacity exceeds consumption within
 * a radius of a given coordinate.
 *
 * @param {number} lat - Latitude of the coordinate to search from.
 * @param {number} lng - Longitude of the coordinate to search from.
 * @param {number} radius - Radius in which to search.
 * @param {'KM' | 'MI'} radiusUnit - The unit that the value of radius is in.
 * @returns {Promise} - a Promise, resolving to an array of site objects.
 */
const findByGeoWithExcessCapacity = async (lat, lng, radius, radiusUnit) => {
  const client = redis.getClient();

  // Create temporary keys for set operations
  const sitesInRadiusSortedSetKey = keyGenerator.getTemporaryKey();
  const sitesInRadiusCapacitySortedSetKey = keyGenerator.getTemporaryKey();

  // Create a pipeline to batch the GEORADIUS and set operations
  const setOperationsPipeline = client.batch();

  // Store sites within radius in a temporary sorted set
  setOperationsPipeline.georadius(
    keyGenerator.getSiteGeoKey(),
    lng,
    lat,
    radius,
    radiusUnit.toLowerCase(),
    'STORE',
    sitesInRadiusSortedSetKey,
  );

  // * ========> START Challenge #5
  // Perform ZINTERSTORE to intersect the sites in radius with the capacity ranking
  // Use WEIGHTS to keep scores from the capacity ranking sorted set
  setOperationsPipeline.zinterstore(
    sitesInRadiusCapacitySortedSetKey,
    2,
    sitesInRadiusSortedSetKey,
    keyGenerator.getCapacityRankingKey(),
    'WEIGHTS',
    0,
    1,
  );
  // * ========> END Challenge #5

  // Set expiration on temporary keys
  setOperationsPipeline.expire(sitesInRadiusSortedSetKey, 30);
  setOperationsPipeline.expire(sitesInRadiusCapacitySortedSetKey, 30);

  // Execute the pipeline
  await setOperationsPipeline.execAsync();

  // Get site IDs with excess capacity (score >= capacityThreshold)
  const siteIds = await client.zrangebyscoreAsync(sitesInRadiusCapacitySortedSetKey, capacityThreshold, '+inf');

  // Retrieve site details for each site ID
  const sites = [];
  for (const siteId of siteIds) {
    const siteKey = keyGenerator.getSiteHashKey(siteId);
    const siteHash = await client.hgetallAsync(siteKey);

    if (siteHash) {
      sites.push(remap(siteHash));
    }
  }

  return sites;
};

module.exports = {
  insert,
  findById,
  findAll,
  findByGeo,
  findByGeoWithExcessCapacity,
};
