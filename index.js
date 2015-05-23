var tfIdf = require("./lib/tf-idf");
var csv = require("./lib/ranking-to-csv");

module.exports = {
  tfIdf: tfIdf,
  csvWriter: csv,
  resources: {
  	donalds: require("../resources/resources-donalds"),
  	nba: require("../resources/resources-nba")
  }
};
