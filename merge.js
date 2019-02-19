/*
// find the package.json from the current folder
// find package.json from all descendent folders
node merge [path]

./package.json
./genes
  /[Address1]
    package.json
  /[Address2]
    package.json

merge Address1/package.json and Address2/package.json into ./package.json

*/
const fs = require('fs')
const path = require('path')
if (process.argv.length === 3) {
  let currentPath = process.argv[2]
  let childPath = currentPath + "/genes"
  let parentFilename = currentPath + "/package.json"
  let files = fs.readdirSync(childPath)
  if (files && files.length > 0) {
    let _parent = require(parentFilename)
    console.log("parent BEFORE override...")
    let before_parent = JSON.stringify(_parent, null, 2)
    console.log(before_parent)
    files.forEach(function(file) {
      let fullPath = path.join(childPath, file);
      if (fs.lstatSync(fullPath).isDirectory()) {
        console.log(fullPath);
        if (fs.existsSync(fullPath)) {
          let p = fullPath + "/package.json"
          console.log("opening", p)
          let _child = require(p)
          if (_child.dependencies) {
            console.log("merging dependencies", _child.dependencies)
            // overwrite dependencies
            Object.keys(_child.dependencies).forEach(function(key) {
              _parent.dependencies[key] = _child.dependencies[key]
            })
          }
        }
      }
    });
    console.log("parent AFTER override...")
    let res = JSON.stringify(_parent, null, 2)
    console.log(res)
    fs.writeFileSync(parentFilename, res)
  }
}
