require('dotenv').config();
const fs = require('fs/promises');
const { BigQuery } = require('@google-cloud/bigquery');
const { Storage } = require('@google-cloud/storage');

const BQ_CLIENT = new BigQuery();
const STORAGE_CLIENT = new Storage();

const DATASET = 'NetSuite';
const BUCKET = 'vnbi-temp-bucket';
const FOLDER = 'NetSuite_dump';

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
};

extract();
