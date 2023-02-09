/**
 * Test runner that spins up and connects multiple nodes
 * of various configurations.
 *
 * For better logging, apply this patch to blgr:
 * https://github.com/bcoin-org/blgr/issues/10
 */
'use strict';

const assert = require('node:assert');
const Logger = require('blgr');
const { FullNode } = require('hsd');
const { NodeClient } = require('hs-client');

/**
 * UTILS
 */

function getRandomInt(min, max) {
  min = Math.ceil(min);
  max = Math.floor(max);
  // The minimum is inclusive and maximum is exclusive
  return Math.floor(Math.random() * (max - min) + min);
}
function randomHost(i) {
  if (i !== undefined)
    return '127.0.0.' + (i + 10);
  return '127.0.0.' + getRandomInt(2, 255);
}
function randomPort(i) {
  if (i !== undefined)
    return 15000 + (i + 10);
  return getRandomInt(10000, 60000);
}

async function forEvent(obj, name, count = 1, timeout = 5000) {
  assert(typeof obj === 'object');
  assert(typeof name === 'string');
  assert(typeof count === 'number');
  assert(typeof timeout === 'number');

  let countdown = count;
  const events = [];

  return new Promise((resolve, reject) => {
    let timeoutHandler, listener;

    const cleanup = function cleanup() {
      clearTimeout(timeoutHandler);
      obj.removeListener(name, listener);
    };

    listener = function listener(...args) {
      events.push({
        event: name,
        values: [...args]
      });

      countdown--;
      if (countdown === 0) {
        cleanup();
        resolve(events);
        return;
      }
    };

    timeoutHandler = setTimeout(() => {
      cleanup();
      const msg = `Timeout waiting for event ${name} `
        + `(received ${count - countdown}/${count})`;

      reject(new Error(msg));
      return;
    }, timeout);

    obj.on(name, listener);
  });
};

class TestHarness {
  constructor() {
    this.network = 'regtest';

    /** @type {import('hsd/lib/node/fullnode')} */
    this.nodes = [];

    this.reverseAddrs = {};

    const logger = new Logger();
    logger.set({
      filename: null,
      level: 'spam',
      console: true,
    });
    this.rootLogger = logger;
    this.logger = logger.context('harness');
  }

  createNodes(num) {
    const apiKey = 'forty-two';
    const listeningNodes = [0, 1, 2];

    for (let i = 0; i < num; i++) {
      const host = randomHost(i);
      const port = randomPort(i);
      const logger = this.rootLogger.context('node-' + i);
      logger.set = () => { };

      const node = new FullNode({
        memory: true,
        network: this.network,
        listen: listeningNodes.includes(i),
        host,
        publicHost: host,
        port,
        publicPort: port,
        brontidePort: randomPort(),
        noDns: true,
        httpHost: host,
        apiKey,
        logger,
        // plugins: [require('hsd/lib/wallet/plugin')],
        // walletHttpPort: randomPort(),
      });

      const client = new NodeClient({
        host,
        port: 14037,
        apiKey,
      });

      this.nodes.push({ host, port, node, client });
    }
  }

  async openNodes() {
    for (const item of this.nodes) {
      const { node } = item;
      if (node.opened) continue;
      await node.open();
      await node.connect();
      node.startSync();
    }
  }

  closeNodes() {
    for (const item of this.nodes) {
      const { node } = item;
      if (!node.opened) continue;
      node.close();
    }
  }

  /**
   * A connects to B
   * @param {number} idxA
   * @param {number} idxB
   */
  async connect(idxA, idxB) {
    this.logger.info('connecting nodes:', idxA, idxB);
    const itemA = this.nodes[idxA];
    const itemB = this.nodes[idxB];

    const poolA = itemA.node.pool;
    const poolB = itemB.node.pool;

    const openPromises = [
      forEvent(poolA, 'peer open', 1, 5000, idxA),
      forEvent(poolB, 'peer open', 1, 5000, idxA),
    ];
    poolA.hosts.addNode(`${itemB.host}:${itemB.port}`);

    const [nodeApeerBevent] = await Promise.all(openPromises);

    const localHostname = nodeApeerBevent[0].values[0].local.hostname;
    this.reverseAddrs[localHostname] = `${itemA.host}:${itemA.port}`;
  }
  displayNodes() {
    console.table(this.nodes.map(item => ({
      host: item.host,
      port: item.port,
      listen: item.node.pool.options.listen,
      peers: item.node.pool.peers.size(),
      list: item.node.pool.peers.list.toArray().map(peer => this.getNodeIdxByHostname(peer.hostname())),
    })));
  }

  getNodeIdxByHostname(hostname) {
    if (this.reverseAddrs[hostname]) {
      hostname = this.reverseAddrs[hostname];
    }

    for (const [idx, item] of this.nodes.entries()) {
      const { host, port } = item;
      if (hostname === `${host}:${port}`)
        return idx;
    }

    return hostname;
  }

  setLoggingLevel(level) {
    this.logger.logger.setLevel(level);
  }
}

(async () => {
  const testHarness = new TestHarness();
  const { logger } = testHarness;

  // Create 5 nodes
  testHarness.createNodes(5);
  await testHarness.openNodes();
  testHarness.displayNodes();

  // Connect some nodes
  await testHarness.connect(0, 1);
  await testHarness.connect(1, 2);
  await testHarness.connect(3, 2);
  testHarness.displayNodes();

  testHarness.logger.info('All connections formed.');

  // Get node info
  // const info = await testHarness.nodes[0].client.getInfo();
  // console.log(info);

  // Get peers
  // const peers = await testHarness.nodes[1].client.execute('getpeerinfo');
  // console.log(peers);

  process.on('SIGINT', async function () {
    logger.info('Closing nodes...')
    testHarness.closeNodes();
    process.exit();
  });
})();
