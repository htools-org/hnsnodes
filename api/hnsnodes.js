const path = require('node:path');
const fs = require('node:fs/promises');
const { BadRequestError, NotFoundError } = require('./utils');


const DIR_NAME_BY_NETWORK = {
  'main': 'mainnet',
  'mainnet': 'mainnet',
  'regtest': 'regtest',
};

const snapshotsMetadata = new Map();

module.exports = (async ({ network }) => {
  // Export directory to read from
  const dirName = DIR_NAME_BY_NETWORK[network];
  if (!network || !dirName) {
    throw new Error(`Invalid network: ${network}`);
  }
  const EXPORT_DIR = path.normalize('../data/export/' + dirName);

  async function getFiles() {
    try {
      const filenames = await fs.readdir(EXPORT_DIR);

      // descending
      filenames.sort((a, b) => {
        if (a > b)
          return -1;
        if (a < b)
          return 1;
        return 0;
      });

      return filenames.filter(name => name.endsWith('.json'));
    } catch (error) {
      console.error(error);
      throw new Error('An unknown error occured.');
    }
  }

  async function getSnapshots() {
    const filenames = await getFiles();
    const res = [];

    try {
      for (const filename of filenames) {
        const f = path.parse(filename);

        let meta = snapshotsMetadata.get(f.name);
        if (!meta) {
          const filepath = path.join(EXPORT_DIR, filename);
          const content = await fs.readFile(filepath, 'utf-8');
          const json = JSON.parse(content);
          const nodesCount = json.length;
          const medianHeight = json.map(x => x[5]).sort()[nodesCount / 2 >>> 0] ?? 0;
          meta = { nodesCount, medianHeight };
          snapshotsMetadata.set(f.name, meta);
        }
        res.push({
          timestamp: f.name,
          url: `/snapshots/${f.name}/`,
          ...meta,
        });
      }
      return res;
    } catch (error) {
      console.error(error);
      throw new Error('An unknown error occured.');
    }
  }

  async function getReachableNodes(snapshotId) {
    const filenames = await getFiles();

    let filename;
    if (snapshotId === 'latest') {
      filename = filenames[0];
    } else {
      filename = snapshotId + '.json';
      if (isNaN(parseInt(snapshotId))) {
        throw new BadRequestError('Invalid snapshot id.');
      }
      if (!filenames.includes(filename)) {
        throw new NotFoundError('Snapshot not found.');
      }
    }

    try {
      const filepath = path.join(EXPORT_DIR, filename);
      const content = await fs.readFile(filepath, 'utf-8');
      const json = JSON.parse(content);

      return json;

      // For reference
      return json.map(el => ({
        addr: el[0],
        port: el[1],
        agent: el[2],
        since: el[3],
        services: el[4],
        height: el[5],
        hostname: el[6],
        geo: {
          city: el[7],
          country: el[8],
          lat: el[9],
          lng: el[10],
          timezone: el[11],
          asn: el[12],
          org: el[13],
        }
      }))
    } catch (error) {
      console.error(error);
      throw new Error('An unknown error occured.');
    }
  }

  // const { newRedisClient } = require('./redis');
  // const client = await newRedisClient();
  // async function queryRedis() {
  //   const up = await client.SMEMBERS('up');
  //   const nodes = up.map(node => node.slice(5).split('-'));
  //   return nodes;
  // }

  return {
    getSnapshots,
    getReachableNodes,
  }
});
