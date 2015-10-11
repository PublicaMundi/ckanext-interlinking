var CKAN = {};

var isNodeModule = (typeof module !== 'undefined' && module != null && typeof require !== 'undefined');

if (isNodeModule) {
  var _ = require('underscore')
    , request = require('request')
    ;
  module.exports = CKAN;
}

(function(my) {
  my.Client = function(endpoint, apiKey) { 
    this.endpoint = _getEndpoint(endpoint);
    this.apiKey = apiKey;
  };

  my.Client.prototype.action = function(name, data, cb) {
    if (name.indexOf('dataset_' === 0)) {
      name = name.replace('dataset_', 'package_');
    }
    var options = {
      url: this.endpoint + '/3/action/' + name,
      data: data,
      type: 'POST'
    };
    return this._ajax(options, cb);
  };

  // make an AJAX request
  my.Client.prototype._ajax = function(options, cb) {
    options.headers = options.headers || {};
    var meth = isNodeModule ? _nodeRequest : _browserRequest;
    return meth(options, cb);
  }

  // Like search but supports ReclineJS style query structure
  //
  // Primarily for use by Recline backend below
  my.Client.prototype.datastoreQuery = function(queryObj, cb) {
    var actualQuery = my._normalizeQuery(queryObj);
    var self = this;
     //console.log('--check 1--') 
     
     /*When it comes to sorting a few things have to be done:
      *  a)Determine if the sorting field refers to the original or the temp_interl resource
      *  b)If the field refers to the temp_interl resource it has to be renamed properly   
      */
          
     var originalQuery;
     var interlinkedSortQueryPart;
     // This variable determines if the original resource enforces order (true) or 
     //   the temp_interlinking one (false) 
     var originalSortMaster;
     
     if(window.dataExplorer !== undefined){
    	 var fields = window.dataExplorer.model.fields;
    	 var sort_query_part = actualQuery.sort.split(' ');
    	 var sort_field = sort_query_part[0];
    	 var sort_direction = sort_query_part[1];
    	 
         interlinkedSortQueryPart = '_id ' + sort_direction;
         originalQuery = actualQuery;
		 originalQuery.sort = '_id ' + sort_direction;
     }
     
     // This function can be used to compare two objects (a,b) based on one of their properties.
     // direction takes values 'asc' and 'desc' with former being the default one.
     function compareObjectsCreator(property, direction){
    	  return function (a, b){
    		  if (a[property] < b[property])
	  	   		    result = -1;
	  	   	  else if (a[property] > b[property])
	  	   			result = 1;
	  	   	  else
	  	   		  return 0;
	  	   	  
	  	   	  if (direction == 'desc'){
	  	   		  return -result
	  	   	  }else{
	  	   		  return result
	  	   	  }
    	  }
     } 
     
    this.action('datastore_search', actualQuery, function(err, original_res_results) {
      if (err) {
        cb(err);
        //console.log('--check 2--')
        return;
      }
      var interlinkingQuery = {
          limit: actualQuery.limit,
          offset: actualQuery.offset,
          //sort: actualQuery.sort,
          sort: interlinkedSortQueryPart,
          resource_id: queryObj.interlinking_resource,
      }
           
      //console.log('--check 3--')
      self.action('datastore_search', interlinkingQuery, function(err2, interlink_res_results) {
          if (err2) {
            cb(err2);
            //console.log('--check 4--')
            return;
          }
	      // map ckan types to our usual types ...
	      var original_res_fields = _.map(original_res_results.result.fields, function(field) {
	        field.type = field.type in my.ckan2JsonTableSchemaTypes ? my.ckan2JsonTableSchemaTypes[field.type] : field.type;
	        return field;
	      });
	      //console.log('--check 5--')
	      var interlink_res_fields = _.map(interlink_res_results.result.fields, function(field) {
	        field.type = field.type in my.ckan2JsonTableSchemaTypes ? my.ckan2JsonTableSchemaTypes[field.type] : field.type;
	        return field;
	      });
	      //console.log('--check 6--')
	        var records = [];
	        var new_fields = [];
	        var fields_temp = interlink_res_fields.slice(0);
	        var interlinked_field_ids = [];

	        // For each original field, add it to final fields, and if you find an 
	        // interlinking field with the same name (but not '_id'), add it after the original one
	        original_res_fields.forEach(function(fld, idx1){
	        	var match_found = false;
	            fields_temp.forEach(function(fld2, idx2){
	                var int_col_id = fld2.id + '_int';
	                
	                if (fld.id == fld2.id && fld.id !== '_id'){
	                	match_found = true;
	                	interlinked_field_ids.push(fld.id)
	                	// The original fields metadata are updated to note that it is under interlinking
	                	var new_fld = fld
	                	// flag declaring that is under ongoing interlinking
	                	new_fld.isInterlinked = true;
	                	// The name of the column in which it corresponds in the datastore
	                	new_fld.dst_column_name = fld.id
	                	new_fields.push(new_fld);
	                	
	                	// This is the field which the users sees. It contains the best interlinking result,
	                	//   or the user's choice if he has chosen a different result
	                	new_fld = {
	                            'id': int_col_id,
	                            'label': fld2.id,
	                            'type': 'text',
	                            // Flag declaring that it hosts interlinking results
	                            'hostsInterlinkingResult': true,
	                            'dst_column_name': fld2.id
	                        };
	                    new_fields.push(new_fld);
	                    
	                    // This field contains the score of the previous one (best result or user's choice).
	                    new_fld = {
	                            'id': fld2.id + '_int_score',
	                    		'label': 'score',
	                            'type': 'text',
	                            'format': 'float-percentage',
	                            'hostsInterlinkingScore': true
	                            // TODO: create a custom renderer	
	                        };
	                    new_fields.push(new_fld);
	                    
	                    // This field contains all results along with their scores.
	                    new_fld = {
	                            'id': fld2.id + '_int_results',
	                            'type': 'text',
	                            'hostsAllInterlinkingResults': true
	                        };
	                    new_fields.push(new_fld);
	                }
	            });
	            //console.log('--check 7--')
	            if(!match_found){
	            	new_fields.push(fld);
	            }

	        });
	        // For each original record, get the respective value of the interlinking records

	        original_res_results.result.records.forEach(function(rc, idx){
	        	interlink_res_fields.forEach(function(fld2, idx2) {
                    if (fld2['id'] !== '_id' && interlinked_field_ids.indexOf(fld2['id']) >= 0){
                    	var int_col_id = fld2.id + '_int';
                    	var int_score_col_name = fld2.id + '_int_score';
                    	var int_results_col_name = fld2.id + '_int_results';
                    	
                    	var val_int = interlink_res_results.result.records[idx][fld2['id']];
                        var val_int_score = interlink_res_results.result.records[idx][(fld2['id'] + '_score')];
                        var val_int_results = interlink_res_results.result.records[idx][(fld2['id'] + '_results')];
                        
                        rc[int_col_id] = val_int;
                        // percentage
                        rc[int_score_col_name] = val_int_score
                        rc[int_results_col_name] = val_int_results; 
                   }
                });
                records.push(rc);
	        });
	        
	        var comfunc = compareObjectsCreator(sort_field,sort_direction)
	        records.sort(comfunc)
	        
	        if (originalSortMaster == true){
	        	;//records.sort(compareObjectsCreator(sort_field,sort_direction))
	        }else{
	        	;
	        }
	        
	        
	        var out = {
	            total: original_res_results.result.total,
	            fields: new_fields,
	            hits: records
	            };
	        
	      cb(null, out);
    });

   });
  };

  my.Client.prototype.datastoreUpdate = function(queryObj, cb) {
    var actualQuery = my._normalizeQuery(queryObj);
    actualQuery['method'] = 'upsert';
    actualQuery['allow_update_with_id'] = true;
    actualQuery['force'] = true;
    var updates = queryObj.updates;
    actualQuery['resource_id'] = queryObj.interlinking_resource;
    var records = [];
    var new_updates = [];
    updates.forEach(function(upd, idx){
    	
    	var it = {};
    	it['_id'] = upd['_id'];
    	for (key in upd){
    		// Looking for the interlinking column
    		var original_col_key = key.substr(0,key.length-4);
    		var score_col_key = key + '_score';
    		var results_col_key = key + '_results';   		
    		if(results_col_key in upd && score_col_key in upd && results_col_key in upd){
        		// The interlinking result column in datastore does not have the '_int' suffix 
        		it[original_col_key] = upd[key];
        		// The interlinking score column in datastore has a plain '_score' suffix (instead of a '_int_score') 
        		it[original_col_key + '_score'] = upd[score_col_key];
    		}
    	}
    	new_updates.push(it);
    });
    actualQuery['records'] = new_updates;
    
    
    this.action('datastore_upsert', actualQuery, function(err, results) {
      if (err) {
        cb(err);
        console.log(err);
        return;
      }

      // map ckan types to our usual types ...
      var fields = _.map(results.result.fields, function(field) {
        field.type = field.type in my.ckan2JsonTableSchemaTypes ? my.ckan2JsonTableSchemaTypes[field.type] : field.type;
        field.state = field.state in my.ckan2JsonTableSchemaTypes ? my.ckan2JsonTableSchemaTypes[field.state] : field.state;
        return field;
      });
      var out = {
        total: results.result.total,
        fields: fields,
        hits: results.result.records
      };
      cb(null, out);
    });
    
    
  };

  my.Client.prototype.datastoreSqlQuery = function(sql, cb) {
    this.action('datastore_search_sql', {sql: sql}, function(err, results) {
      if (err) {
        var parsed = JSON.parse(err.message);
        var errOut = {
          original: err,
          code: err.code,
          message: parsed.error.info.orig[0]
        };
        cb(errOut);
        return;
      }

      // map ckan types to our usual types ...
      var fields = _.map(results.result.fields, function(field) {
        field.type = field.type in my.ckan2JsonTableSchemaTypes ? my.ckan2JsonTableSchemaTypes[field.type] : field.type;
        return field;
      });
      var out = {
        total: results.result.length,
        fields: fields,
        hits: results.result.records
      };
      cb(null, out);
    });
  };

  my.ckan2JsonTableSchemaTypes = {
    'text': 'string',
    'int': 'integer',
    'int4': 'integer',
    'int8': 'integer',
    'float8': 'float',
    'timestamp': 'datetime',
    'bool': 'boolean',
  };

  // 
  my.jsonTableSchema2CkanTypes = {
    'string': 'text',
    'number': 'float',
    'integer': 'int',
    'datetime': 'timestamp',
    'boolean': 'bool',
    'binary': 'bytea',
    'object': 'json',
    'array': 'text[]',
    'any': 'text'
  };

  // list all the resources with an entry in the DataStore
  my.Client.prototype.datastoreResources = function(cb) {
    var data = {
      resource_id: '_table_metadata'
    };
    return this.action('datastore_search', data, cb);
  };

  // Utilities
  // =========

  var _getEndpoint = function(endpoint) {
    endpoint = endpoint || '/';
    // strip trailing /
    endpoint = endpoint.replace(/\/$/, '');
    if (!endpoint.match(/\/api$/)) {
      endpoint += '/api';
    }
    return endpoint;
  };

  var _nodeRequest = function(options, cb) {
    var conf = {
      url: options.url,
      headers: options.headers || {},
      method: options.type || 'GET',
      json: options.data
    };
    // we could just call request but that's a PITA to mock plus request.get = request (if you look at the source code)
    request(conf, function(err, res, body) {
      if (!err && res && !(res.statusCode === 200 || res.statusCode === 302)) {
        err = 'CKANJS API Error. HTTP code ' + res.statusCode + '. Message: ' + JSON.stringify(body, null, 2);
      }
      cb(err, body);
    });
  };

  var _browserRequest = function(options, cb) {
    var self = this;
    options.data = encodeURIComponent(JSON.stringify(options.data));
    options.success = function(data) {
      cb(null, data);
    }
    options.error = function(obj, obj2, obj3) {
      var err = {
        code: obj.status,
        message: obj.responseText
      }
      cb(err); 
    }
    if (options.headers) {
      options.beforeSend = function(req) {
        for (key in options.headers) {
          req.setRequestHeader(key, options.headers[key]);
        }
      };
    }
    return jQuery.ajax(options);
  };

  // only put in the module namespace so we can access for tests!
  my._normalizeQuery = function(queryObj) {
    var actualQuery = {
      resource_id: queryObj.resource_id,
      q: queryObj.q,
      filters: {},
      limit: queryObj.size || 10,
      offset: queryObj.from || 0
    };

    if (queryObj.sort && queryObj.sort.length > 0) {
      var _tmp = _.map(queryObj.sort, function(sortObj) {
        return sortObj.field + ' ' + (sortObj.order || '');
      });
      actualQuery.sort = _tmp.join(',');
    }

    if (queryObj.filters && queryObj.filters.length > 0) {
      _.each(queryObj.filters, function(filter) {
        if (filter.type === "term") {
          actualQuery.filters[filter.field] = filter.term;
        }
      });
    }
    return actualQuery;
  };

  // Parse a normal CKAN resource URL and return API endpoint etc
  //
  // Normal URL is something like http://demo.ckan.org/dataset/some-dataset/resource/eb23e809-ccbb-4ad1-820a-19586fc4bebd
  //
  // :return: { resource_id: ..., endpoint: ... }
  my.parseCkanResourceUrl = function(url) {
    parts = url.split('/');
    var len = parts.length;
    return {
      resource_id: parts[len-1],
      endpoint: parts.slice(0,[len-4]).join('/') + '/api'
    };
  };
}(CKAN));


// Recline Wrapper
//
// Wrap the DataStore to create a Backend suitable for usage in ReclineJS
//
// This provides connection to the CKAN DataStore (v2)
//
// General notes
// 
// We need 2 things to make most requests:
//
// 1. CKAN API endpoint
// 2. ID of resource for which request is being made
//
// There are 2 ways to specify this information.
//
// EITHER (checked in order): 
//
// * Every dataset must have an id equal to its resource id on the CKAN instance
// * The dataset has an endpoint attribute pointing to the CKAN API endpoint
//
// OR:
// 
// Set the url attribute of the dataset to point to the Resource on the CKAN instance. The endpoint and id will then be automatically computed.
var recline = recline || {};
recline.Backend = recline.Backend || {};
recline.Backend.CkanInterlinkEdit = recline.Backend.CkanInterlinkEdit || {};
(function(my) {
  my.__type__ = 'ckanInterlinkEdit';

  // private - use either jQuery or Underscore Deferred depending on what is available
  var Deferred = _.isUndefined(this.jQuery) ? _.Deferred : jQuery.Deferred;
    
  // ### fetch
  my.fetch = function(dataset) {
	  /*
	  console.log('------inside FETCH!!!------')
      */
    var dfd = new Deferred();
    my.query({}, dataset)
      .done(function(data) {
        dfd.resolve({
          fields: data.fields,
          records: data.hits
        });
      })
      .fail(function(err) {
        dfd.reject(err);
      })
      ;
    return dfd.promise();
  };

  my.query = function(queryObj, dataset) {
	  //console.log('------inside QUERY!!!------')
    var dfd = new Deferred()
      , wrapper
      ;
        
        var q = [ {field: '_id', order: 'asc'}];
        //resort with _id
        if (queryObj['sort'] === undefined){
            queryObj['sort'] = q;
        }
    if (dataset.endpoint) {
      wrapper = new CKAN.Client(dataset.endpoint);
    } else {
      var out = CKAN.parseCkanResourceUrl(dataset.url);
      dataset.id = out.resource_id;
      wrapper = new CKAN.Client(out.endpoint);
    }
    queryObj.resource_id = dataset.id;
    queryObj.interlinking_resource = dataset.temp_interlinking_resource; 
        
    wrapper.datastoreQuery(queryObj, function(err, out) {
      if (err) {
        dfd.reject(err);
      } else {
        dfd.resolve(out);
      }
    });
    return dfd.promise();
  };

  my.save = function(queryObj, dataset) {
      var dfd = new Deferred(), wrapper;
      if (dataset.endpoint) {
          wrapper = new CKAN.Client(dataset.endpoint);
      }
      else {
          var out = CKAN.parseCkanResourceUrl(dataset.url);
          dataset.id = out.resource_id;
          wrapper = new CKAN.Client(out.endpoint);
      }
      queryObj.resource_id = dataset.id;

     queryObj.interlinking_resource = dataset.temp_interlinking_resource; 
     wrapper.datastoreUpdate(queryObj,function(err, out){
     if (err) {
         console.log(err);
     }
     });
      //var dfd = new Deferred();
      //msg = 'Saving more than one item at a time not yet supported';
      //alert(msg);
      //dfd.reject(msg);
      queryObj.updates = [];
      return dfd.promise();

  };
}(recline.Backend.CkanInterlinkEdit));

