var CKAN = {};


var isNodeModule = (typeof module !== 'undefined' && module != null && typeof require !== 'undefined');

if (isNodeModule) {
  var _ = require('underscore')
    , request = require('request')
    ;
  module.exports = CKAN;
}

(function foo01(my) {
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
    /*
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
     
     
    this.action('datastore_search', actualQuery, function(err, original_res_results) {
      if (err) {
        cb(err);
        return;
      }
      var interlinkingQuery = {
          limit: actualQuery.limit,
          offset: actualQuery.offset,
          //sort: actualQuery.sort,
          sort: interlinkedSortQueryPart,
          resource_id: queryObj.interlinking_resource,
          interlinked_column: queryObj.interlinked_column,
      }
                 
      self.action('datastore_search', interlinkingQuery, function(err2, interlink_res_results) {
          if (err2) {
            cb(err2);
            return;
          }
     
	      // map ckan types to our usual types ...
	      var original_res_fields = _.map(original_res_results.result.fields, function(field) {
	        field.type = field.type in my.ckan2JsonTableSchemaTypes ? my.ckan2JsonTableSchemaTypes[field.type] : field.type;
	        return field;
	      });

	      var interlink_res_fields = _.map(interlink_res_results.result.fields, function(field) {
	        field.type = field.type in my.ckan2JsonTableSchemaTypes ? my.ckan2JsonTableSchemaTypes[field.type] : field.type;
	        return field;
	      });
	      	      	
	        var records = [];
	        var new_fields = [];
	        if (typeof interlink_res_results != 'undefined'){
	        	var fields_temp = interlink_res_fields.slice(0);
	        }
	        var int_field_ids = [];

	        interlinking_utility.int_state['interlinked_column'] = interlinking_utility.int_state['interlinked_column'] || interlinkingQuery.interlinked_column; 
	        var interlinked_column_name = interlinking_utility.int_state['interlinked_column'];
	        
	        // For each original field, add it to final fields, and if you find an 
	        // interlinking field with the same name (but not '_id'), add it after the original one
	        interlinking_utility.int_state['backend_columns'] = {};
	        original_res_fields.forEach(function(fld, idx1){
	        	var match_found = false;
	        	var current_field;
	        	if (typeof interlinked_column_name != 'undefined' && 
	        				fld.id == interlinked_column_name && 
	        				interlink_res_results.result.records.length > 0 &&
	        				fld.id !== '_id'){
	        		new_fields.push(fld);
	        		// This is the field which the users sees. It contains the best interlinking result,
	                //   or the user's choice if he has chosen a different result 
	        		current_field = fields_temp[1];
	        		interlinking_utility.int_state['interlinking_temp_column'] = current_field.id;
	        		var new_fld_id = interlinked_column_name + '_int'
	        		new_fld = {
	                            'id': new_fld_id,
	                            'label': current_field.id,
	                            'type': 'text',
	                            // Flag declaring that it hosts interlinking results
	                            'hostsInterlinkingResult': true,
	                            'dst_column_name': current_field.id
	                        };
	        		new_fields.push(new_fld);
	        		int_field_ids.push(current_field.id);
	        		
	        		// This field contains the score of the previous one (best result or user's choice).
	        		current_field = fields_temp[2];
	        		new_fld_id = interlinked_column_name + '_int_score'
                    new_fld = {
                            'id': new_fld_id,
                    		'label': 'score',
                            'type': 'text',
                            'format': 'float',
                            'hostsInterlinkingScore': true
                            // TODO: create a custom renderer	
                        };
                    new_fields.push(new_fld);
                    int_field_ids.push(current_field.id);
                                      
	        		
                    // This field contains all results along with their scores.
                    current_field = fields_temp[3];
                    new_fld_id = interlinked_column_name + '_int_results'
                    new_fld = {
                            'id': new_fld_id,
                            'type': 'text',
                            'hostsAllInterlinkingResults': true
                        };
                    new_fields.push(new_fld);
                    int_field_ids.push(current_field.id);
                    
                    // This field is a boolean flag indicating if the user has checked (parcticaly clicked on) 
                    //  an interlinking result by hand 
                    current_field = fields_temp[4];
                    new_fld_id = interlinked_column_name + '_int_checked'
                    new_fld = {
                            'id': new_fld_id,
                            'type': 'boolean',
                            'label': 'edit',
                            'hostsInterlinkinCheckedFlag': true,
                            'format': 'boolean',
                        };
                    new_fields.push(new_fld);
                    int_field_ids.push(current_field.id);
                    
	        		
                    // Adding the rest of the reference fields
                    interlinking_utility.int_state['auxiliary_columns'] = [];
	        		fields_temp.forEach(function(fld2, idx2){
	        			if (idx2 >= 5 ){
	        				current_field = fld2;
	        				new_fld_id = interlinked_column_name + '_int_aux_' + fld2.id;
	        				new_fld = {
	                                'id': new_fld_id,
	                                'label': fld2.id,
	                                'type': 'text',
	                                'hostsInterlinkingAuxField': true
	                            };
	        				new_fields.push(new_fld);
	        				int_field_ids.push(current_field.id);
	        			}
		            });
	        	}
	            if(!match_found){
	            	new_fields.push(fld);
	            }

	        });
	        /*
	        console.log(interlink_res_results)
	        console.log(interlink_res_results.result)
	        console.log(interlink_res_results.result.records[0])
	        console.log(int_field_ids[0])
	        console.log(interlink_res_results.result.records[0][int_field_ids[0]])
	        console.log(interlinked_column_name)
	        */
	        // For each original record, get the respective value of the interlinking records
	        original_res_results.result.records.forEach(function(rc, idx){
	        	if (typeof interlinked_column_name != 'undefined' && interlink_res_results.result.records.length > 0){
	        		
		        	// main interlinking field
		        	var col_id = interlinked_column_name + '_int';
		        	var val_int = interlink_res_results.result.records[idx][int_field_ids[0]];
		        	rc[col_id] = val_int;
		        	
	        		// score interlinking field
		        	col_id = interlinked_column_name + '_int_score';
		        	val_int = interlink_res_results.result.records[idx][int_field_ids[1]];
		        	rc[col_id] = val_int;
		        	
	        		// checked flag field
		        	col_id = interlinked_column_name + '_int_checked';
		        	val_int = interlink_res_results.result.records[idx][int_field_ids[2]];
		        	rc[col_id] = val_int;
		        	
	        		// interlinking results field
		        	col_id = interlinked_column_name + '_int_results';
		        	val_int = interlink_res_results.result.records[idx][int_field_ids[3]];
		        	rc[col_id] = val_int;
	
	        		
		        	// auxiliary fields from the reference dataset
		        	interlink_res_fields.forEach(function(fld2, idx2) {
		        		if (idx2 >= 5){
		        			col_id = interlinked_column_name + '_int_aux_' + int_field_ids[idx2-1];
		    	        	val_int = interlink_res_results.result.records[idx][int_field_ids[idx2-1]];
		        			rc[col_id] = val_int;
		        		}
	                });
	        	}
                records.push(rc);
	        });
	        
	        var comfunc = my._compareObjectsCreator(sort_field,sort_direction)
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
    
	
    // Getting fields info
    var datastore_field_names = {}
    var interlinked_column;
    var interlinking_column;
    var score_column;
    var results_column;
    var original_fields = []
    for (var key in updates[0]){
    	if (key.indexOf('_int') > 0 && key+'_score' in updates[0] && 
    			key+'_results' in updates[0] && key+ '_checked' in updates[0]){
    		interlinking_column = key
    		interlinked_column = interlinking_utility.int_state['interlinked_column'];
    		score_column = key + '_score';
    		results_column = key + '_results';
    		checked_column = key + '_checked';
    		original_fields = JSON.parse(updates[0][key + '_results'])['fields']
    		break;
    	}
    }
    if(updates.length > 0){
	    datastore_field_names[interlinking_column] = original_fields[0];
	    datastore_field_names[score_column] = 'int__score';
	    datastore_field_names[results_column] = 'int__all_results';
	    datastore_field_names[checked_column] = 'int__checked_flag';
	    for (var i=2; i< original_fields.length; i++){
	    	datastore_field_names[interlinked_column + '_int_aux_' + original_fields[i]] = original_fields[i];
	    }
    }
     
    updates.forEach(function(upd, idx){  	
    	var it = {};
    	it['_id'] = upd['_id'];
    	
    	for (var dfn in datastore_field_names){
    		it[datastore_field_names[dfn]] = upd[dfn];
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

  var _getEndpoint = function foo(endpoint) {
    endpoint = endpoint || '/';
    // strip trailing /
    endpoint = endpoint.replace(/\/$/, '');
    if (!endpoint.match(/\/api$/)) {
      endpoint += '/api';
    }
    return endpoint;
  };

  var _nodeRequest = function foo(options, cb) {
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

  var _browserRequest = function foo(options, cb) {
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
  my._normalizeQuery = function foo(queryObj) {
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
  
  // This function can be used to compare two objects (a,b) based on one of their properties.
  // direction takes values 'asc' and 'desc' with the former being the default one.
  my._compareObjectsCreator = function foo(property, direction){
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

  // Parse a normal CKAN resource URL and return API endpoint etc
  //
  // Normal URL is something like http://demo.ckan.org/dataset/some-dataset/resource/eb23e809-ccbb-4ad1-820a-19586fc4bebd
  //
  // :return: { resource_id: ..., endpoint: ... }
  my.parseCkanResourceUrl = function foo(url) {
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
(function foo02(my) {
  my.__type__ = 'ckanInterlinkEdit';

  // private - use either jQuery or Underscore Deferred depending on what is available
  var Deferred = _.isUndefined(this.jQuery) ? _.Deferred : jQuery.Deferred;
    
  // ### fetch
  my.fetch = function(dataset) {
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
    queryObj.interlinked_column = dataset.interlinked_column;
        
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

