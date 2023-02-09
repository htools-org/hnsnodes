const express = require('express');
const helmet = require('helmet');

const config = require('./config');

(async () => {
  const network = config.network ?? 'main';
  const hnsnodes = await require('./hnsnodes')({ network });

  const app = express();
  app.use(helmet())

  app.get('/', (req, res) => {
    return res.redirect('https://github.com/htools-org/hnsnodes');
  });

  app.get('/snapshots', async (req, res) => {
    const data = await hnsnodes.getSnapshots();
    return res.json({
      status: 'success',
      data: data,
    });
  });

  app.get('/snapshots/:snapshotId/reachable', async (req, res) => {
    try {
      const snapshotId = req.params.snapshotId;
      const data = await hnsnodes.getReachableNodes(snapshotId);
      return res.json({
        status: 'success',
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
    console.log(`HnsNodes API listening on port ${port}.`);
  });
})();
