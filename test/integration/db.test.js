'use strict'

const assert = require('assert')
const { sortBy } = require('lodash')
const Db = require('../../db')
const GENES = require('../genes')
const { mongoClient, resetDatabase } = require('../helper')

describe('db', () => {
  const PLANA_DB_URL = process.env.PLANA_DB_URL
  if (!PLANA_DB_URL) {
    throw new Error('Cannot run tests without PLANA_DB_URL set')
  }
  resetDatabase()
  describe('init()', () => {
    it('should initiate mongo db connections based on input genes', async () => {
      const result = await Db.init(GENES)
      assert.strictEqual(result.length, 2)
      assert.deepEqual(result, [undefined, undefined])
    })
    it('should return an empty array when feeding with empty genes array', async () => {
      const result = await Db.init([])
      assert.strictEqual(result.length, 0)
    })
  })
  describe('exit()', () => {
    it('should close all mongo db connections', async () => {
      await Db.init(GENES)
      const result = await Db.exit()
      assert.strictEqual(result.length, 2)
      assert.deepEqual(result, [undefined, undefined])
    })
    it('should throw error when db has not been initialized before', () => {
      Db.exit()
        .then(
          () => Promise.reject(new Error('Expected method to reject.')),
          err => assert(err instanceof Error)
        )
    })
  })
  describe('instances()', () => {
    it('should return infos about the initialized instances', async () => {
      await Db.init(GENES)
      const instances = await Db.instances()
      assert.strictEqual(instances.length, 2)
      assert.deepEqual(sortBy(instances, i => i.address), [
        { address: '14iSjRBVYf5mnMb5RbjJ1wxAM5sG1Ae3K1', names: [] },
        { address: '1DH8b6PtgLFrgtKvmDVCf9pmTUbWZMiV9F', names: [] }
      ])
    })
    it('should return empty info array when feeding with empty genes array', async () => {
      await Db.init([])
      const instances = await Db.instances()
      assert.strictEqual(instances.length, 0)
    })
  })
  describe('index()', () => {
    beforeEach('Init db', async () => await Db.init(GENES))
    afterEach('Exit db', async () => await Db.exit())
    it('should create collections and indexes', async () => {
      await Db.index()
      const client = await mongoClient()
      for (const gene of GENES) {
        const collections = await client.db(gene.address).collections()
        assert.deepEqual(
          sortBy(collections.map(c => c.collectionName)),
          sortBy([...Object.keys(gene.index), 'system.indexes'])
        )
      }
      // TODO: assert existence of indezies
    })
  })
})
