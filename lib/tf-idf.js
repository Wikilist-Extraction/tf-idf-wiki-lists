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

var getLabelOf = function(type, callback) {
  var query = "select ?label from $graph where { <$type> rdfs:label ?label. FILTER (langMatches(lang(?label),'en')) }";
  var sQuery = dbpediaSparqler.createQuery(query);

  sQuery
    .setParameter("graph", graph)
    .setParameter("type", encodeURI(type))
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
  var typePromise = new $.Deferred();

  var dfds = _.map(
    listOfResources,
  	function(resource, index) { 
  		var dfd = new $.Deferred();

      var flattenResult = function(body) {
        var result = {
          resource: resource,
          types: _(dbpediaSparqler.sparqlFlatten(body))
            .mapValues("type")
            .map(function(typeUri) { return typeUri; })
            .value()
        };
        dfd.resolve(result);
      };

      var updateAvailableResourcesCount = function(typeUris) {
        if (_.isEmpty(typeUris))
          return typeUris;

        amountOfAvailableResources++;
        return typeUris;
      };

      timeoutWrapper(function() { getTypesOf(resource, flattenResult); }, index);

  		return dfd.then(updateAvailableResourcesCount);
  	}
  );

  $.when.apply(this, dfds).done(function() { typePromise.resolve(arguments); });

  return typePromise;
};

var getTfIdfFromTypes = function(listsOfTypeUris) {
  var tfIdfPromise = new $.Deferred();

  var index = 0;
  var countedListOfTypes = _(listsOfTypeUris)
    .flatten()
    .pluck("types")
    .flatten()
    .countBy()
    .value();

  var mapTypeUriToEntities = _(countedListOfTypes)
    .mapValues(function(count, typeUri) {
      return _(listsOfTypeUris)
        .filter(function(result) {
          return _.contains(result.types, typeUri);
        })
        .pluck("resource")
        .value();
    })
    .value();

  var dfds = _.map(
    countedListOfTypes,
    function(count, typeUri) {
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
              overallFrac: overallFrac,
              entities: mapTypeUriToEntities[typeUri]
            }
          );
        });
      }, REQUEST_THRESHOLD * index);

      index++;
      return dfd;
    }
  );

  $.when.apply(this, dfds).done(function() { tfIdfPromise.resolve(arguments); });
    
  return tfIdfPromise;
};

var getLabelsForTfIdfResults = function(listOfTfIdfs) {
  var labelPromise = new $.Deferred();

  var dfds =  _.map(
    listOfTfIdfs,
    function(result, index) { 
      var dfd = new $.Deferred();

      var flattenResult = function(body) {
        var label = _(dbpediaSparqler.sparqlFlatten(body))
          .mapValues("label")
          .map(function(label) { return label; })
          .value()[0];

        dfd.resolve(
          _.extend(result, { label: label })
        );
      };

      timeoutWrapper(function() { getLabelOf(result.typeUri, flattenResult); }, index);

      return dfd;
    }
  );

  $.when.apply(this, dfds).done(function() { labelPromise.resolve(arguments); });

  return labelPromise;
};

var filterResultsForValidLabels = function(results) {
  return _.filter(results, function(result) {
    return typeof result.label === "string";
  });
};

module.exports = function(listOfResources, doneCallback) {
  if (!listOfResources.length || listOfResources.listOfResources === 0) {
    throw new Error("listOfResources has to be an array");
  }

  if (typeof doneCallback != "function") {
    throw new Error("doneCallback has to be a function");
  }

  getTypesForEntities(listOfResources)
    .then(getTfIdfFromTypes)
    .then(getLabelsForTfIdfResults)
    .then(filterResultsForValidLabels)
    .done(function(results) {
      doneCallback(
        results, 
        { 
          present: amountOfAvailableResources, 
          missing: listOfResources - amountOfAvailableResources, 
          count: listOfResources.length 
        }
      );
    });
};

