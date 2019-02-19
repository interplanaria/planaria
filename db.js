const _ = require('highland');
const MongoClient = require('mongodb').MongoClient
var db
var mongo
var GENES
var Manager
const init = function(genes) {
  GENES = genes
  Manager = {} 
  let promises = GENES.map(function(gene, index) {
    return new Promise(function(resolve) {
      MongoClient.connect(process.env.PLANA_DB_URL, {useNewUrlParser: true}, function(err, client) {
        if (err) console.log(err)
        Manager[gene.address] = {
          db: client.db(gene.address),
          mongo: client
        }
        resolve()
      })
    })
  })
  return Promise.all(promises)
}
const exit = function() {
  let promises = Object.keys(Manager).map(function(address) {
    return new Promise(function(resolve) {
      Manager[address].mongo.close()
      resolve()
    })
  })
  return Promise.all(promises)
}

/***************************************
*
*  instances()
*
*  returns:
*
*  [{
*    address: [Bitcoin Address],
*    names: [Array of Collection Names]
*  }, {
*    address: [Bitcoin Address],
*    names: [Array of Collection Names]
*  }]
*
***************************************/
const instances = async function() {
  return Object.keys(Manager).map(async function(address) {
    let infos = await Manager[address].db.listCollections().toArray()
    let collectionNames = infos.map(function(info) { return info.name })
    return {
      address: address,
      names: collectionNames
    }
  })
}

/***********************************
*
*   1. Create
*
*     // Single item insert
*     db.create({
*       address: PLANARIA_ADDRESS,
*       name: COLLECTION_NAME,
*       data: DOCUMENT,
*       onchunk: function(chunk) { },
*       onfinish: function() { },
*     })
*   
*     // Multi item batch insert
*     db.create({
*       address: PLANARIA_ADDRESS,
*       name: COLLECTION_NAME,
*       data: DOCUMENT_ARRAY,
*       onchunk: function(chunk) { },
*       onfinish: function() { },
*     })
*   
*
*   2. Read
*
*     db.read({
*       address: PLANARIA_ADDRESS,
*       name: COLLECTION_NAME,
*       filter: {
*         find: FIND_FILTER,
*         project: PROJECT,
*         sort: SORT,
*         limit: LIMIT,
*         skip: SKIP,
*       }
*     })
*   
*
*   3. Update: Delete and Insert
*
*     db.update({
*       address: PLANARIA_ADDRESS,
*       name: COLLECTION_NAME,
*       filter: {
*         find: FIND_FILTER,
*         project: PROJECT,
*         sort: SORT,
*         limit: LIMIT,
*         skip: SKIP,
*       },
*       map: MAP_FUNCTION
*     })
*   
*
*   4. Delete
*
*     db.delete({
*       address: PLANARIA_ADDRESS,
*       name: COLLECTION_NAME,
*       filter: {
*         find: FIND_FILTER,
*       }
*     })
*
***********************************/


const _create = function(o) {
  let db = Manager[o.address].db
  if (Array.isArray(o.data)) {
    // batch insert
    return new Promise(function(resolve, reject) {
      let items = o
      let insertMany = _.wrapCallback(function(chunk, callback) {
        db.collection(o.name).insertMany(chunk, { ordered: false }, callback)
      })
      _(o.data).batch(1000).map(insertMany).sequence()
      .errors(function(err) {
        if (err.writeErrors) {
          console.log("$e", JSON.stringify(err.writeErrors, null, 2))
        } else {
          console.log("$ Error", JSON.stringify(err, null, 2))
        }
        reject(err)
      })
      .toArray(function(x) {
        console.log("batch")
        resolve()
      })
    })
  } else {
    return db.collection(o.name).insertMany([o.data])
  }
}
const _read = function(o) {
  if (o.address && o.filter && o.filter.find && o.name) {
    let db = Manager[o.address].db
    let cursor = db.collection(o.name).find(o.filter.find)
    if (o.filter.sort) cursor = cursor.sort(o.filter.sort)
    if (o.filter.project) cursor = cursor.project(o.filter.project)
    if (o.filter.skip) cursor = cursor.skip(o.filter.skip)
    if (o.filter.limit) cursor = cursor.limit(o.filter.limit)
    return cursor.toArray()
  } else {
    return new Promise(function(resolve, reject) {
      reject({ error: "need address, filter, find, name" })
    })
  }
}
// update: delete + create
const _update = function(o) {
  console.log("update", o)
  return _read(o).then(function(txs) {
    console.log("read", o)
    let mapped = (o.map ? txs.map(o.map) : txs)
    return _delete(o).then(function() {
      return _create({
        address: o.address,
        name: o.name,
        data: mapped,
      })
    })
  })
}
const _delete = function(o) {
  console.log("delete", o)
  if (o.address && o.name && o.filter && o.filter.find) {
    let db = Manager[o.address].db
    return db.collection(o.name).deleteMany(o.filter.find)
  } else {
    return new Promise(function(resolve, reject) {
      reject({ error: "Need address, name, filter, and find" })
    })
  }
}
const index = async function() {
  console.log('\n\n* Indexing MongoDB...')
  console.time('TotalIndex')
  for(let j=0; j<GENES.length; j++) {
    let gene = GENES[j]
    let db = Manager[gene.address].db
    if (gene.index) {
      let collectionNames = Object.keys(gene.index)
      for(let j=0; j<collectionNames.length; j++) {
        let collectionName = collectionNames[j]
        let keys = gene.index[collectionName].keys
        let uniq = gene.index[collectionName].unique
        let fulltext = gene.index[collectionName].fulltext
        if (keys) {
          console.log('Indexing keys...')
          if (Array.isArray(keys)) {
            // basic
            for(let i=0; i<keys.length; i++) {
              let o = {}
              o[keys[i]] = 1
              console.time('Index:' + keys[i])
              try {
                if (uniq && uniq.includes(keys[i])) {
                  await db.collection(collectionName).createIndex(o, { unique: true })
                  console.log('* Created unique index for ', keys[i])
                } else {
                  await db.collection(collectionName).createIndex(o)
                  console.log('* Created index for ', keys[i])
                }
              } catch (e) {
                console.log("Index already exists", keys[i])
                process.exit()
              }
              console.timeEnd('Index:' + keys[i])
            }
          } else {
            // object
            let k = Object.keys(keys)
            for(let i=0; i<k.length; i++) {
              let o = {}
              let key = k[i]
              o[key] = keys[key]
              console.time('Index:' + key)
              try {
                if (uniq && uniq.includes(key)) {
                  await db.collection(collectionName).createIndex(o, { unique: true })
                  console.log('* Created unique index for ', key)
                } else {
                  await db.collection(collectionName).createIndex(o)
                  console.log('* Created index for ', key)
                }
              } catch (e) {
                console.log("Index already exists", key)
                process.exit()
              }
              console.timeEnd('Index:' + key)
            }
          }
        }
        if (fulltext) {
          console.log('Creating full text index...')
          let o = {}
          fulltext.forEach(function(key) {
            o[key] = 'text'
          })
          console.time('Fulltext search for ' + collectionName, o)
          try {
            await db.collection(collectionName).createIndex(o, { name: 'fulltext' })
          } catch (e) {
            console.log("Index already exists: full text for", collectionName)
            process.exit()
          }
          console.timeEnd('Fulltext search for ' + collectionName)
        }
      }
    }
  }
  console.log('* Finished indexing MongoDB...\n\n')
  console.timeEnd('TotalIndex')
}
module.exports = {
  init: init, exit: exit,
  instances: instances,
  create: _create, read: _read, update: _update, delete: _delete,
  index: index
}
