#! /usr/bin/env node

var tfIdf = require("../lib/tf-idf");
var csv = require("../lib/ranking-to-csv");

var listOfResources = require("../resources/resources-nba");

console.log("START COMPUTING TF-IDF");

tfIdf(listOfResources, function(ranking, counts) {
	console.log("available resources", counts.present, "tested resources", counts.count);
	console.log("WRITING TO entities.csv ...");
	csv(ranking, "entities.csv");
});
