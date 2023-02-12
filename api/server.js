const express = require('express');
const helmet = require('helmet');
const cors = require('cors');

const config = require('./config');

(async () => {
  const network = config.network ?? 'main';
  const hnsnodes = await require('./hnsnodes')({ network });

  const app = express();
  app.use(helmet());
  app.use(cors());

  // Analytics
  if (config.umami?.websiteId) {
    console.info('Tracking with umami.');
    app.use((req, res, next) => {
      fetch(config.umami.collectUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'User-Agent': req.headers['user-agent'],
        },
        body: JSON.stringify({
          payload: {
            website: config.umami.websiteId,
            url: req.path,
            referrer: req.headers.referer,
            hostname: req.hostname,
          },
          type: 'pageview',
        }),
      }).catch(console.error);
      next();
    });
  }

  app.get('/', (req, res) => {
    return res.redirect('https://github.com/htools-org/hnsnodes');
  });

  app.get('/v1/snapshots', async (req, res) => {
    const before = req.query.before;
    const limit = req.query.limit;
    try {
      const data = await hnsnodes.getSnapshots(before, limit);
      return res.json({
        status: 'success',
        keys: ['timestamp', 'medianHeight', 'nodesCount'],
        data: data,
      });
    } catch (error) {
      return res.status(error.statusCode ?? 500).json({
        status: 'error',
        message: error.message,
      })
    }
  });

  app.get('/v1/snapshots/:snapshotId/reachable', async (req, res) => {
    try {
      const snapshotId = req.params.snapshotId;
      const data = await hnsnodes.getReachableNodes(snapshotId);
      return res.json({
        status: 'success',
        keys: [
          'addr', 'port', 'agent', 'since', 'services', 'height', 'hostname',
          'city', 'country', 'lat', 'lng', 'timezone', 'asn', 'org',
        ],
        data: data,
      });
    } catch (error) {
      return res.status(error.statusCode ?? 500).json({
        status: 'error',
        message: error.message,
      })
    }
  });

  const port = process.env.PORT ?? config.port ?? 3000;
  app.listen(port, () => {
    console.info(`HnsNodes API listening on port ${port}.`);
  });
})();
