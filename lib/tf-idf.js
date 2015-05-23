// dependencies
var sparqler = require("sparqler");
var _ = require("lodash");
var $ = require("jquery-deferred");

var REQUEST_THRESHOLD = 50;
var PREFETCHED_COUNT_OF_ALL_RESOURCES = 883644351; // select count(?s) where {?s ?p ?o}
var amountOfAvailableResources = 0;

var dbpediaPrefixes = require("../resources/prefixes");
var dbpediaSparqler = new sparqler.Sparqler("http://dbpedia.org/sparql");
var graph = "<http://dbpedia.org>";

var getTypesOf = function(uri, callback) {
	var query = "select ?type from $graph where { <$uri> rdf:type ?type }";
	var sQuery = dbpediaSparqler.createQuery(query);

	sQuery
    .setParameter("graph", graph)
		.setParameter("uri", encodeURI(uri))
		.execute(callback);
};

var getCountOf = function(type, callback) {
  var query = "select COUNT(?o) as ?count from $graph where { ?o rdf:type <$type> }";
  var sQuery = dbpediaSparqler.createQuery(query);

  sQuery
    .setParameter("graph", graph)
    .setParameter("type", type)
    .execute(callback);
};

var getTypeFromTypeUri = function(typeUri) {
 for (var i in dbpediaPrefixes) {
   if (typeUri.contains(dbpediaPrefixes[i].uri)) {
     return typeUri.replace(dbpediaPrefixes[i].uri, dbpediaPrefixes[i].prefix + ":");
   }
 }

 return typeUri;
};

var timeoutWrapper = function(callback, index) {
  if (typeof callback != "function") {
    throw new Error("callback has to be a function");
  }

  if (typeof index != "number") {
    throw new Error("index has to be a number");
  }

  return setTimeout(callback, index * REQUEST_THRESHOLD);
};

var getTypesForEntities = function(listOfResources) { 
  return _(listOfResources)
  	.map(function(resource, index) { 
  		var dfd = new $.Deferred();

      var flattenResult = function(body) {
        dfd.resolve(
          _(dbpediaSparqler.sparqlFlatten(body))
            .mapValues("type")
            .map(function(typeUri) { return typeUri; })
            .value()
        );
      };

      var updateAvailableResourcesCount = function(typeUris) {
        if (_.isEmpty(typeUris))
          return typeUris;

        amountOfAvailableResources++;
        return typeUris;
      };

      timeoutWrapper(function() { getTypesOf(resource, flattenResult); }, index);

  		return dfd.then(updateAvailableResourcesCount);
  	})
  	.value();
};

var getTfIdfFromTypes = function(typePromises) {
  var tfIdfPromise = new $.Deferred();

  $.when.apply(this, typePromises).done(function() {

    var index = 0;
  	var countedListOfTypes = _(arguments)
  		.flatten()
  		.countBy()
      .value();

    var countedPromises = _(countedListOfTypes)
      .map(function(count, typeUri) {
        var dfd = new $.Deferred();

        setTimeout(function() {
          getCountOf(typeUri, function(body) {
            var _overallCount;
            var overallFrac = _(dbpediaSparqler.sparqlFlatten(body))
                .mapValues("count")
                .map(function(overallCount) {
                  _overallCount = overallCount;
                  return count / parseInt(overallCount);
                })
                .value()[0];

            var idf = Math.log(PREFETCHED_COUNT_OF_ALL_RESOURCES /  _overallCount);
            var tf = count / amountOfAvailableResources;

            dfd.resolve(
              { 
                type: getTypeFromTypeUri(typeUri), 
                typeUri: typeUri, 
                count: count, 
                idf: idf,
                tf: tf,
                tfIdf: idf * tf, 
                overallFrac: overallFrac
              }
            );
          });
        }, 50 * index);

        index++;
        return dfd;
      })
      .value();

    tfIdfPromise.resolve(countedPromises);
  });

  return tfIdfPromise;
};

module.exports = function(listOfResources, doneCallback) {
  if (!listOfResources.length || listOfResources.listOfResources === 0) {
    throw new Error("listOfResources has to be an array");
  }

  if (typeof doneCallback != "function") {
    throw new Error("doneCallback has to be a function");
  }

  var typePromises = getTypesForEntities(listOfResources);
  var tfIdfPromise = getTfIdfFromTypes(typePromises);

  tfIdfPromise.done(function(tfIdfPromises) {
    $.when.apply(this, tfIdfPromises).done(function() {
      doneCallback(
        arguments, 
        { 
          present: amountOfAvailableResources, 
          missing: listOfResources - amountOfAvailableResources, 
          count: listOfResources.length 
        }
      );
    });
  });
};

