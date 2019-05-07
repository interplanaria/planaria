const assert = require('assert')
const Db = require('../../db')

describe('db', () => {
  describe('init', () => {
    it('should invoke \'connect\' method of mongodb client', async () => {
      await assert.doesNotReject(Db.init([]))
    })
  })
})
