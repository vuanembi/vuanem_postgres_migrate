require('dotenv').config();
const fs = require('fs/promises');
const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');
const { Pool } = require('pg');

const BQ_CLIENT = new BigQuery();
const STORAGE_CLIENT = new Storage();

const DATASET = 'NetSuite';
const BUCKET = 'vnbi-temp-bucket';
const FOLDER = 'NetSuite_dump';

const PG_POOL = new Pool();

const extract = async () => {
  const { tables } = JSON.parse(await fs.readFile('tables.json'));
  const options = {
    location: 'US',
    format: 'csv',
  };
  await Promise.all(
    tables.map(async (table) => {
      const filename = `${FOLDER}/${table}/${table}-*.csv`;
      return BQ_CLIENT.dataset(DATASET)
        .table(table)
        .extract(STORAGE_CLIENT.bucket(BUCKET).file(filename), options);
    }),
  );
  console.log('extracted');
};

const load = async (tables) => {
  await PG_POOL.connect();
  await Promise.all(
    tables.map(async (table) => {
      const truncateQuery = `TRUNCATE TABLE "${DATASET}"."${table}"`;
      await PG_POOL.query(truncateQuery);
      const files = await fs.readdir(`${FOLDER}/${table}`);
      await Promise.all(
        files.map(async (file) => {
          const copyQuery = `
            COPY "${DATASET}"."${table}"
            FROM '${__dirname}/${FOLDER}/${table}/${file}'
            DELIMITER ','
            CSV HEADER;`;
          await PG_POOL.query(copyQuery);
        }),
      );
    }),
  );
};

const main = async () => {
  const args = process.argv.slice(2);
  const { tables } = JSON.parse(await fs.readFile('tables.json'));
  if (args[0] === 'extract') {
    await extract(tables);
  } else if (args[0] === 'load') {
    await load(tables);
  } else {
    console.log(__dirname);
  }
};

main();
