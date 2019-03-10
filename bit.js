var Db
var Info
var rpc
var filter
var processor
var working = false
// Const
const BITCOIN_CONFIG = {
  rpc: {
    protocol: 'http',
    user: process.env.BITCOIN_USER,
    pass: process.env.BITCOIN_PASSWORD,
    host: (process.env.HOST ? process.env.HOST : ip.address()),
    port: process.env.BITCOIN_PORT,
    limit: parseInt(process.env.BITCOIN_CONCURRENCY)
  },
  zmq: {
    host: (process.env.HOST ? process.env.HOST : ip.address()),
    port: process.env.BITCOIN_ZMQ_PORT
  }
}
const PLANA_CONFIG = {
  zmq: {
    host: process.env.PLANA_ZMQ_HOST,
    port: process.env.PLANA_ZMQ_PORT,
  }
}
// Dependencies
const zmq = require('zeromq')
const RpcClient = require('bitcoind-rpc')
const TXO = require('txo')
const fs = require('fs')
const pLimit = require('p-limit')
const pQueue = require('p-queue')
const queue = new pQueue({concurrency: BITCOIN_CONFIG.rpc.limit})
const bcode = require('bcode')
const path = require('path')
var GENES

const init = function(db, info, genes) {
  return new Promise(function(resolve) {
    GENES = genes
    Db = db
    Info = info
    rpc = new RpcClient(BITCOIN_CONFIG.rpc)
    resolve()
  })
}
const request = {
  block: function(block_index) {
    return new Promise(function(resolve) {
      rpc.getBlockHash(block_index, function(err, res) {
        if (err) {
          console.log('Err = ', err)
          throw new Error(err)
        } else {
          rpc.getBlock(res.result, function(err, block) {
            resolve(block)
          })
        }
      })
    })
  },
  /**
  * Return the current blockchain height
  */
  height: function() {
    return new Promise(function(resolve) {
      rpc.getBlockCount(function(err, res) {
        if (err) {
          console.log('Err = ', err)
          throw new Error(err)
        } else {
          resolve(res.result)
        }
      })
    })
  },
  tx: async function(hash, verbose) {
    let content = await TXO.fromHash(hash, verbose, BITCOIN_CONFIG.rpc)
    delete content.tx.r // don't use raw tx for now. implement memory efficient handling first
    return content
  },
  mempool: function() {
    return new Promise(function(resolve) {
      rpc.getRawMemPool(async function(err, ret) {
        if (err) {
          console.log('Err', err)
        } else {
          let tasks = []
          const limit = pLimit(BITCOIN_CONFIG.rpc.limit)
          let txs = ret.result
          console.log('txs = ', txs.length)
          for(let i=0; i<txs.length; i++) {
            tasks.push(limit(async function() {
              let content = await request.tx(txs[i]).catch(function(e) {
                console.log('Error = ', e)
              })
              return content
            }))
          }
          let btxs = await Promise.all(tasks)
          resolve(btxs)
        }
      })
    })
  }
}
const crawl = async function(block_index) {
  let block_content = await request.block(block_index)
  let block_hash = block_content.result.hash
  let block_time = block_content.result.time
  if (block_content && block_content.result) {
    let txs = block_content.result.tx
    console.log('crawling txs = ', txs.length)
    let tasks = []
    const limit = pLimit(BITCOIN_CONFIG.rpc.limit)
    for(let i=0; i<txs.length; i++) {
      tasks.push(limit(async function() {
        let t = await request.tx(txs[i]).catch(function(e) {
          console.log('Error = ', e)
        })
        t.blk = { i: block_index, h: block_hash, t: block_time }
        return t
      }))
    }
    let btxs = await Promise.all(tasks)
    btxs = bcode.decode(btxs)
    console.log('Block ' + block_index + ' : ' + txs.length + 'txs | ' + btxs.length + ' filtered txs')

    // set coinbase text for the first tx
    let t = await request.tx(txs[0], true).catch(function(e) {
      console.log('Error = ', e)
    })
    if (t.coinbase) {
      block_content.result.coinbase = t.coinbase
    }
    return {
      info: block_content.result,
      items: btxs
    }
  } else {
    return {
      info: block_content.result,
      items: []
    }
  }
}
const outsock = zmq.socket('pub')
const publish = function(address, topic, obj) {
  console.log("[pub] ", address, topic)
  let envelope = {
    data: obj,
    address: address
  }
  let msg = JSON.stringify(envelope)
  outsock.send([topic, msg])
}
const listen = function() {
  let sock = zmq.socket('sub')
  sock.connect('tcp://' + BITCOIN_CONFIG.zmq.host + ':' + BITCOIN_CONFIG.zmq.port)
  sock.subscribe('hashtx')
  sock.subscribe('hashblock')
  console.log('Subscriber connected to port ' + BITCOIN_CONFIG.zmq.port)

  console.log("BindSync ", "tcp://*:" + PLANA_CONFIG.zmq.port)
  outsock.bindSync('tcp://*:' + PLANA_CONFIG.zmq.port)
  console.log('Started publishing to tcp://*:' + PLANA_CONFIG.zmq.port)

  // Listen to ZMQ
  sock.on('message', async function(topic, message) {
    if (topic.toString() === 'hashtx') {
      let hash = message.toString('hex')
      console.log('New tx hash from ZMQ = ', hash)
      await sync('tx', hash)
    } else if (topic.toString() === 'hashblock') {
      let hash = message.toString('hex')
      console.log('New block hash from ZMQ = ', hash)
      await sync('block')
    }
  })

  // Don't trust ZMQ. Try synchronizing every 1 minute in case ZMQ didn't fire
  setInterval(async function() {
    await sync('block')
  }, 60000)

}

const sync = async function(type, hash) {
  if (type === 'block') {
    if (!working) {
      try {
        // semaphore for when a sync(block) is triggered
        // when another sync(block) is still working
        working = true

        // lastSynchronized is the minimum of all machine clocks
        let lastSynchronized = null
        for(let i=0; i<GENES.length; i++) {
          let gene = GENES[i]
          let clk = await Info.getclock(gene)
          lastSynchronized = (lastSynchronized === null ? clk : Math.min(lastSynchronized, clk))
        }
        const currentHeight = await request.height()
        console.log('$$ Block Current Height = ', currentHeight)
        console.log('$$ lastSynchronized = ', lastSynchronized)

        // lastSynchronized will be same as currentHeight
        // if this call was made by setTimeout
        // and we're still ini the same block as last call
        if (lastSynchronized < currentHeight) {
          // clear mempool and synchronize
          // only if the block height is NOT the same as last time
          // otherwise it will end up refreshing mempool DB too often.
          for(let index=lastSynchronized+1; index<=currentHeight; index++) {
            console.log('RPC BEGIN ' + index, new Date().toString())
            console.time('RPC END ' + index)
            let block = await crawl(index)
            console.timeEnd('RPC END ' + index)

            console.log(new Date().toString())
            console.log('DB BEGIN ' + index, new Date().toString())
            console.time('DB Insert ' + index)

            let mempool = []
            console.log("index = ", index)
            console.log("currentHeight = ", currentHeight)
            if (index === currentHeight) {
              console.log('Clear mempool and repopulate')
              mempool = await request.mempool()
            }

            for(let i=0; i<GENES.length; i++) {
              if (GENES[i].onblock) {
                let gene = GENES[i]
                let clk = await Info.getclock(gene)
                /*******************************************************************************
                *
                *  MACHINE := {
                *    input: {
                *      block: {
                *        items: [ <Transaction in TXO Format>, ..., <Transaction in TXO Format> ],
                *        info: {
                *          "hash": "000000000000000000410544d7cfe8f90b81daa1f02469a40be99e455c671be7",
                *          "confirmations": 151461,
                *          "size": 853803,
                *          "height": 411501,
                *          "version": 4,
                *          "versionHex": "00000004",
                *          "merkleroot": "efeb98352db1ad0ff691a8d76a552134f464648e09cc598e9b850b9aebbb8fa6",
                *          "tx": [
                *            "d14bb27028ce113f26b1ddee282479c573729b500dd3eda9249ccca360cf576e",
                *            "5432ca41143c74dc6dbb5551c817a50901fe1559b963f8a9731656f9136f52d2",
                *            "3ac66bbf21ad279223cab8b166f4c74621f98eff6ffb4cbf3fd080da88b5bb23",
                *            "3865e7facad32626dcd45584f830ce0ae439008a5271aa828158a96cd38b5de4",
                *            "8b3244fb3cd991bc216d27d11f863f44e55ae8f1f5d9655714a0b4caa408f573",
                *            "0143601e1a1621e94e0802fe44112a6a7f6ed889128dab537531158b8c27d565",
                *          ],
                *          "time": 1463095538,
                *          "mediantime": 1463093385,
                *          "nonce": 4098576299,
                *          "bits": "1805a8fa",
                *          "difficulty": 194254820283.44,
                *          "chainwork": "00000000000000000000000000000000000000000019172cdab3f28b3ad7c0ec",
                *          "previousblockhash": "00000000000000000258adbacc58c8acd91d492943ad64ab740ca5fa74b7f2a6",
                *          "nextblockhash": "000000000000000000b66d9af7a90a4335c59384f0c7cef926a63d4303a65811",
                *          "coinbase": "036d47061e4d696e656420627920416e74506f6f6c20626a3020201a535520573510efe0030000ea5c5581"
                *        }
                *      },
                *      mempool: {
                *        items: [ <Transaction in TXO Format>, ..., <Transaction in TXO Format> ]
                *      }
                *    },
                *    output: {
                *      publish: function(topic, obj) { [publish stringified object to zmq topic] }
                *    },
                *    state: <Db Interface>,
                *    clock: {
                *      bitcoin: {
                *        now: <Bitcoin Height>,
                *      },
                *      self: {
                *        now: <Database Height>,
                *      },
                *    }
                *  }
                *
                *******************************************************************************/
                let MACHINE = {
                  input: {
                    block: block,
                    mempool: { items: mempool }
                  },
                  output: {
                    publish: function(o) {
                      publish(gene.address, o.name, o.data)
                    }
                  },
                  state: {
                    create: function(o) {
                      return Db.create(Object.assign({address: gene.address}, o))
                    },
                    read: function(o) {
                      return Db.read(Object.assign({address: gene.address}, o))
                    },
                    update: function(o) {
                      return Db.update(Object.assign({address: gene.address}, o))
                    },
                    delete: function(o) {
                      return Db.delete(Object.assign({address: gene.address}, o))
                    },
                  },
                  clock: {
                    bitcoin: {
                      now: currentHeight,
                    },
                    self: {
                      now: clk
                    }
                  },
                  assets: {
                    path: './public/assets/' + gene.address,
                    absolutePath: path.resolve('./public/assets/' + gene.address)
                  }
                }
                // if the gene's checkpoint is larger than the global checkpoint
                // that means this gene doesn't need to be crawled
                // so skip it
                if (clk < index) {
                  console.log("onblock", gene.address, clk, 'of', index)
                  // if the machine clock is smaller than the current crawling checkpoint
                  // it means machine is out of sync, so need to crawl
                  await gene.onblock(MACHINE)
                  // update the machine clock after onblock completes
                  await Info.setclock(gene, index)
                } else {
                  console.log("skip onblock", gene.address, clk, 'of', index)
                }
              }
            }
            console.timeEnd('DB Insert ' + index)
            console.log('------------------------------------------')
            console.log('\n')
          }
        }
        // finished working
        working = false
        if (lastSynchronized === currentHeight) {
          console.log('no update')
          return null
        } else {
          console.log('[finished]')
          return currentHeight
        }
      } catch (e) {
        console.log('Error', e)
        console.log('Shutting down...', new Date().toString())
        await Db.exit()
        process.exit()
      }
    }
  } else if (type === 'tx') {
    queue.add(async function() {
      let o = await request.tx(hash, true)
      // distinguish between mempool transactions and block transactions
      if (!o.confirmations || (o.confirmations && o.confirmations === 0)) {
        try {
          delete o.confirmations
        } catch (e) { }

        let decoded = bcode.decode([o])
        try {
          for(let i=0; i<GENES.length; i++) {
            /*******************************************************************************
            *
            *   MACHINE := {
            *     input: <Transaction in TXO Format>,
            *     output: {
            *      publish: function(topic, obj) { [publish stringified object to zmq topic] }
            *     },
            *     state: <Database>
            *   }
            *
            *******************************************************************************/
            let MACHINE = {
              input: decoded[0],
              output: {
                publish: function(o) {
                  publish(GENES[i].address, o.name, o.data)
                }
              },
              state: {
                create: function(o) {
                  return Db.create(Object.assign({address: GENES[i].address}, o))
                },
                read: function(o) {
                  return Db.read(Object.assign({address: GENES[i].address}, o))
                },
                update: function(o) {
                  return Db.update(Object.assign({address: GENES[i].address}, o))
                },
                delete: function(o) {
                  return Db.delete(Object.assign({address: GENES[i].address}, o))
                },
              },
              assets: {
                path: './public/assets/' + GENES[i].address,
                absolutePath: path.resolve('./public/assets/' + GENES[i].address)
              }
            }
            if (GENES[i].onmempool) {
              await GENES[i].onmempool(MACHINE)
            }
          }
          console.log('# Q inserted [size: ' + queue.size + ']',  hash)
        } catch (e) {
          // duplicates are ok because they will be ignored
          if (e.code == 11000) {
            console.log('Duplicate mempool item: ', o)
          } else {
            console.log('## ERR ', e, o)
            process.exit()
          }
        }
      } else {
        console.log("Not mempool")
      }
    })
    return hash
  }
}
const run = async function() {
  await sync('block')
}
module.exports = {
  init: init,
  crawl: crawl,
  listen: listen,
  request: request,
  sync: sync,
  run: run
}
