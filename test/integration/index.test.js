const assert = require('assert')

describe('planaria', () => {
  describe('start', () => {
    it('should fail to start', () => {
      assert.throws(() => require('../..'))
    })
  })
})
