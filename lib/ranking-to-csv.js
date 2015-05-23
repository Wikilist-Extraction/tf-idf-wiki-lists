var csv = require("fast-csv");
var fs = require("fs");

module.exports = function(dataArray, filename) {
  var ws = fs.createWriteStream(filename)
    .on("finish", function() { console.log("done writing csv"); });
  
  csv
    .write(dataArray, {headers: true})
    .pipe(ws);
};
