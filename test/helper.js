'use strict'

const MongoClient = require('mongodb').MongoClient
const GENES = require('./genes')

function mongoClient() {
  return new Promise((resolve, reject) => {
    MongoClient.connect(process.env.PLANA_DB_URL, { useNewUrlParser: true }, (err, client) => {
      if (err) {
        reject(err)
        return
      }
      resolve(client)
    })
  })
}

function resetDatabase() {
  beforeEach('Reset database', async () => {
    const client = await mongoClient()
    await Promise.all(GENES.map(gene => client.db(gene.address).dropDatabase()))
    await client.close()
  })
}

module.exports = {
  mongoClient,
  resetDatabase
}
