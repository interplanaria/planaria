console.log("\n\n####################")
console.log("#")
console.log("# Welcome to Planaria")
console.log("#")
console.log("####################\n\n")
// ENV
const fs = require('fs')
const path = require('path');
const dotenv = require('dotenv')
dotenv.config({path: path.resolve(process.cwd(), 'planaria.env')})
console.log("####################")
console.log("# ENV...")
console.log("#", JSON.stringify(process.env, null, 2))
console.log("#")
try {
  const override = dotenv.parse(fs.readFileSync('child.env'))
  for (let k in override) {
    process.env[k] = override[k]
    console.log("# Overriding", k, ":", override[k])
  }
} catch (e) { }
console.log("####################")

// Filter the folders in the current directory that has:
// 1. has planaria.js
// 2. the planaria.js file is valid
// 3. assign them to GENES
var GENES = []
var currentPath = process.cwd()
var genePath = currentPath + "/genes"
var dirs = fs.readdirSync(genePath).filter(function (file) {
  return fs.statSync(genePath+'/'+file).isDirectory();
});
dirs.forEach(function(_path) {
  let pth = genePath + "/" + _path
  console.log("Reading", pth)
  fs.readdirSync(pth).forEach(function(file) {
    if (file === 'planaria.js') {
      let f = require(pth + "/" + file)
      if (f) {
        console.log("Found planaria.js")
        GENES.push(f)
      } else {
        console.log("planaria.js Not found")
      }
    }
  })
})
console.log("GENES = ", GENES)
const Info = require('./info.js')
const Bit = require('./bit.js')
const Db = require('./db')
const ip = require('ip')
const path = require('path')
const daemon = {
  run: async function() {
    // 1. Initialize
    await Db.init(GENES)
    await Bit.init(Db, Info, GENES)
    // Use the most recent block height-1 if "from" doesnt exist
    // (because it starts crawling from clock+1)
    let height = await Bit.request.height()
    GENES.forEach(function(gene) {
      if (gene.from === null || typeof gene.from === 'undefined') {
        gene.from = height-1
      }
    })
    await Info.init(GENES)

    // 2. Index
    console.log('Indexing...', new Date())
    console.time('Indexing Keys')
    await Db.index()
    console.timeEnd('Indexing Keys')

    // 3. Rewind
    for(let i=0; i<GENES.length; i++) {
      let gene = GENES[i]
      let clk = await Info.getclock(gene)
      // rewind (onrestart) shouldn't be triggered the first time
      // only trigger if the clock is larger than "from"
      if (clk > gene.from) {
        await util.rewind(gene, clk)
      }
    }

    // 4. Start synchronizing
    console.log('Synchronizing...', new Date())
    console.time('Initial Sync')
    await Bit.run()
    console.timeEnd('Initial Sync')

    // 5. Start listening
    Bit.listen()
  }
}
const util = {
  run: async function() {
    await Info.init(GENES)
    await Db.init(GENES)
    let cmd = process.argv[2]
    if (cmd === 'fix') {
      // Fix all: node index --max-old-space-size=4096 fix 560000
      // Fix one machine: node index --max-old-space-size=4096 fix 560000 [ADDR1]
      // Fix multiple machines: node index --max-old-space-size=4096 fix 560000 [ADDR1],[ADDR2]
      if (process.argv.length > 3) {
        let from = parseInt(process.argv[3])
        await Bit.init(Db, Info, GENES)
        let addrs = null
        if (process.argv.length > 4) {
          addrs = process.argv[4].split(',')
        }

        // 2. Rewind
        console.log("Rewinding Machines to", from)
        for(let i=0; i<GENES.length; i++) {
          let gene = GENES[i]
          // there is no address specified, or if the gene address is included in the arg
          if (!addrs || addrs.includes(gene.address)) {
            // fix gene, starting from index "from"
            console.log("Rewinding", gene.address)
            await util.rewind(gene, from)
            // set the clock for the selected genes to "from"
            await Info.setclock(gene, from)
            console.log("Rewind Finished for", gene.address)
          }
        }
        console.log("All Rewinds Complete")

        // 3. Start synchronizing
        console.log('Synchronizing...', new Date())
        console.time('Initial Sync')
        await Bit.run()
        console.timeEnd('Initial Sync')

        // 4. Start listening
        Bit.listen()
      }
    } else if (cmd === 'index') {
      await Db.index()
      process.exit()
    }
  },
  /*
  *
  * "rewind" is an interface that Planaria node developers must implement
  * in order to take care of rewinding and cleaning up in case of restarts
  *
  * "rewind to X" means rewind the state back to the last time when block X was successfully crawled.
  *
  * Takes:
  * - gene: the gene object
  * - from: the index to onrestart from
  *
  * Programmers must handle restart logic in "onrestart" which involves:
  * 1. Deleting all the items after the clock.self.now point
  * 2. Setting the clock to clock.self.now
  *
  */
  rewind: async function(gene, from) {
    if (from < gene.from) {
      // if trying to rewind past gene.from
      // set 'from' as gene.from-1
      from = gene.from-1
    }
    console.log('Restarting from index ', from)
    let currentHeight = await Bit.request.height()
    if (gene.onrestart) {
      let MACHINE = {
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
            now: from,
            set: function(val) {
              return Info.setclock(gene, val)
            }
          },
        },
        assets: {
          path: './public/assets/' + gene.address,
          absolutePath: path.resolve('./public/assets/' + gene.address)
        }
      }
      await gene.onrestart(MACHINE)
    }
    console.log('[finished]')
  }
}
const start = async function() {
  if (process.argv.length > 2) {
    util.run()
  } else {
    daemon.run()
  }
}
start()
