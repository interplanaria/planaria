const level = require('level')
const interlevel = require('interlevel')
const kv = level('./.state', {
  valueEncoding: 'json'
})
var server
var GENES
const init = function(genes) {
  console.log("Info.init")
  GENES = genes
  return new Promise(function(resolve, reject) {
    console.log("# storing genes", GENES)
    kv.put('genes', GENES, function(err) {
      console.log("Returned")
      if (err) {
        console.log("Error", err)
        reject()
      } else {
        console.log("Initializing Interlevel Server")
        server = interlevel.server({ db: kv, port: 28337 })
        console.log("Initialized", server)
        resolve()
      }
    })
  })
}
/**
*
*   getclock:
*   1. returns the 'from' from the gene if it's first time
*   2. returns the last crawled clock point if it's not the first time
*
*/
const getclock = function(gene) {
  let from = gene.from
  let addr = gene.address
  return new Promise(function(resolve, reject) {
    kv.get(addr, function(err, value) {
      if (err) {
        if (err.notFound) {
          // the first time, it's not found, so the clock is set to 'from-1'
          // because clock represents the checkpoint to which the last crawl
          // has completed
          kv.get('tip', function(err, value) {
            if (err) {
              console.log('Tip not found. Returning the "from":', from-1)
              resolve(from-1)
            } else {
              console.log('Tip found. Returning the "from":', value)
              let cp = parseInt(value)
              resolve(cp)
            }
          })
          //console.log('Clock not found. Returning the "from":', from)
          //resolve(from-1)
        } else {
          console.log('err', err)
          reject(err)
        }
      } else {
        // if already set from previous sessions, return the value
        let clk = parseInt(value)
        console.log('Clk found,', clk)
        resolve(clk)
      }
    })
  })
}
/**
*
*   setclock:
*   sets clock for the machine specified by the gene
*
*/
const setclock = function(gene, index) {
  let addr = gene.address
  return new Promise(function(resolve, reject) {
    kv.put(addr, index, function(err) {
      if (err) {
        console.log(err)
        reject(err)
      } else {
        kv.put('clock', index, function(err) {
          if (err) console.log(err)
        })
        resolve()
      }
    })
  })
}
/**
* Return the last synchronized checkpoint
*/
const checkpoint = function() {
  return new Promise(async function(resolve, reject) {
    kv.get('tip', function(err, value) {
      if (err) {
        if (err.notFound) {
          // start from the min value
          console.log('Checkpoint not found, checking for the smallest GENESIS point')
          let min
          for(let i=0; i<GENES.length; i++) {
            let g = GENES[i]
            if (i === 0) {
              min = g.from
            } else {
              if (g.from < min) min = g.from
            }
            console.log(g.from)
          }
          console.log("Found minimum:", min)
          kv.put('clock', min, function(err) {
            if (err) console.log(err)
          })
          resolve(min)
        } else {
          console.log('err', err)
          reject(err)
        }
      } else {
        let cp = parseInt(value)
        console.log('Checkpoint found,', cp)
        kv.put('clock', cp, function(err) {
          if (err) console.log(err)
        })
        resolve(cp)
      }
    })
  })
}
const updateTip = function(index) {
  return new Promise(function(resolve, reject) {
    kv.put('tip', index, function(err) {
      if (err) {
        console.log(err)
        reject()
      } else {
        console.log('Tip updated to', index)
        kv.put('clock', index, function(err) {
          if (err) console.log(err)
        })
        resolve()
      }
    })
  })
}
const deleteTip = function() {
  return new Promise(function(resolve, reject) {
    kv.del('tip', function(err) {
      if (err) {
        console.log(err)
        reject()
      } else {
        console.log('Tip deleted')
        kv.put('clock', null, function(err) {
          if (err) console.log(err)
        })
        resolve()
      }
    })
  })
}
module.exports = {
  init: init,
  getclock: getclock,
  setclock: setclock,
  checkpoint: checkpoint,
  updateTip: updateTip,
  deleteTip: deleteTip
}
