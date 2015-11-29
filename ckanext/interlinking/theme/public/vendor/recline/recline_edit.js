this.recline = this.recline || {};
this.recline.Backend = this.recline.Backend || {};
this.recline.Backend.Ckan = this.recline.Backend.Ckan || {};

// Initialize  this.recline.Backend.Ckan. 
// TODO: check not needed
(function foo01(my) {
  // ## CKAN Backend
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

  my.__type__ = 'ckan';

  // private - use either jQuery or Underscore Deferred depending on what is available
  var Deferred = _.isUndefined(this.jQuery) ? _.Deferred : jQuery.Deferred;

  // Default CKAN API endpoint used for requests (you can change this but it will affect every request!)
  //
  // DEPRECATION: this will be removed in v0.7. Please set endpoint attribute on dataset instead
  my.API_ENDPOINT = 'http://datahub.io/api';

  // ### fetch
  my.fetch = function (dataset) {
    var wrapper;
    if (dataset.endpoint) {
      wrapper = my.DataStore(dataset.endpoint);
    } else {
      var out = my._parseCkanResourceUrl(dataset.url);
      dataset.id = out.resource_id;
      wrapper = my.DataStore(out.endpoint);
    }
    var dfd = new Deferred();
    var jqxhr = wrapper.search({resource_id: dataset.id, limit: 0});
    jqxhr.done(function(results) {
      // map ckan types to our usual types ...
      var fields = _.map(results.result.fields, function(field) {
        field.type = field.type in CKAN_TYPES_MAP ? CKAN_TYPES_MAP[field.type] : field.type;
        return field;
      });
      var out = {
        fields: fields,
        useMemoryStore: false
      };
      dfd.resolve(out);  
    });
    return dfd.promise();
  };

  // only put in the module namespace so we can access for tests!
  my._normalizeQuery = function(queryObj, dataset) {
    var actualQuery = {
      resource_id: dataset.id,
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

  my.query = function(queryObj, dataset) {
    var wrapper;
    if (dataset.endpoint) {
      wrapper = my.DataStore(dataset.endpoint);
    } else {
      var out = my._parseCkanResourceUrl(dataset.url);
      dataset.id = out.resource_id;
      wrapper = my.DataStore(out.endpoint);
    }
    var actualQuery = my._normalizeQuery(queryObj, dataset);
    var dfd = new Deferred();
    var jqxhr = wrapper.search(actualQuery);
    jqxhr.done(function(results) {
      var out = {
        total: results.result.total,
        hits: results.result.records
      };
      dfd.resolve(out);  
    });
    return dfd.promise();
  };

  // ### DataStore
  //
  // Simple wrapper around the CKAN DataStore API
  //
  // @param endpoint: CKAN api endpoint (e.g. http://datahub.io/api)
  my.DataStore = function(endpoint) { 
    var that = {endpoint: endpoint || my.API_ENDPOINT};

    that.search = function(data) {
      var searchUrl = that.endpoint + '/3/action/datastore_search';
      var jqxhr = jQuery.ajax({
        url: searchUrl,
        type: 'POST',
        data: JSON.stringify(data)
      });
      return jqxhr;
    };

    
    return that;
  };

  // Parse a normal CKAN resource URL and return API endpoint etc
  //
  // Normal URL is something like http://demo.ckan.org/dataset/some-dataset/resource/eb23e809-ccbb-4ad1-820a-19586fc4bebd
  my._parseCkanResourceUrl = function(url) {
    parts = url.split('/');
    var len = parts.length;
    return {
      resource_id: parts[len-1],
      endpoint: parts.slice(0,[len-4]).join('/') + '/api'
    };
  };

  var CKAN_TYPES_MAP = {
    'int4': 'integer',
    'int8': 'integer',
    'float8': 'float'
  };

}(this.recline.Backend.Ckan));

this.recline = this.recline || {};
this.recline.Backend = this.recline.Backend || {};
this.recline.Backend.DataProxy = this.recline.Backend.DataProxy || {};

(function foo03(my) {
  my.__type__ = 'dataproxy';
  // URL for the dataproxy
  my.dataproxy_url = 'http://jsonpdataproxy.appspot.com';
  // Timeout for dataproxy (after this time if no response we error)
  // Needed because use JSONP so do not receive e.g. 500 errors 
  my.timeout = 5000;

  
  // use either jQuery or Underscore Deferred depending on what is available
  var Deferred = _.isUndefined(this.jQuery) ? _.Deferred : jQuery.Deferred;

  // ## load
  //
  // Load data from a URL via the [DataProxy](http://github.com/okfn/dataproxy).
  //
  // Returns array of field names and array of arrays for records
  my.fetch = function(dataset) {
    var data = {
      url: dataset.url,
      'max-results':  dataset.size || dataset.rows || 1000,
      type: dataset.format || ''
    };
    var jqxhr = jQuery.ajax({
      url: my.dataproxy_url,
      data: data,
      dataType: 'jsonp'
    });
    var dfd = new Deferred();
    _wrapInTimeout(jqxhr).done(function(results) {
      if (results.error) {
        dfd.reject(results.error);
      }

      dfd.resolve({
        records: results.data,
        fields: results.fields,
        useMemoryStore: true
      });
    })
    .fail(function(args) {
      dfd.reject(args);
    });
    return dfd.promise();
  };

  // ## _wrapInTimeout
  // 
  // Convenience method providing a crude way to catch backend errors on JSONP calls.
  // Many of backends use JSONP and so will not get error messages and this is
  // a crude way to catch those errors.
  var _wrapInTimeout = function(ourFunction) {
    var dfd = new Deferred();
    var timer = setTimeout(function() {
      dfd.reject({
        message: 'Request Error: Backend did not respond after ' + (my.timeout / 1000) + ' seconds'
      });
    }, my.timeout);
    ourFunction.done(function(args) {
        clearTimeout(timer);
        dfd.resolve(args);
      })
      .fail(function(args) {
        clearTimeout(timer);
        dfd.reject(args);
      })
      ;
    return dfd.promise();
  };

}(this.recline.Backend.DataProxy));


// This file adds in full array method support in browsers that don't support it
// see: http://stackoverflow.com/questions/2790001/fixing-javascript-array-functions-in-internet-explorer-indexof-foreach-etc

// Add ECMA262-5 Array methods if not supported natively
if (!('indexOf' in Array.prototype)) {
    Array.prototype.indexOf= function(find, i /*opt*/) {
        if (i===undefined) i= 0;
        if (i<0) i+= this.length;
        if (i<0) i= 0;
        for (var n= this.length; i<n; i++)
            if (i in this && this[i]===find)
                return i;
        return -1;
    };
}
if (!('lastIndexOf' in Array.prototype)) {
    Array.prototype.lastIndexOf= function(find, i /*opt*/) {
        if (i===undefined) i= this.length-1;
        if (i<0) i+= this.length;
        if (i>this.length-1) i= this.length-1;
        for (i++; i-->0;) /* i++ because from-argument is sadly inclusive */
            if (i in this && this[i]===find)
                return i;
        return -1;
    };
}
if (!('forEach' in Array.prototype)) {
    Array.prototype.forEach= function(action, that /*opt*/) {
        for (var i= 0, n= this.length; i<n; i++)
            if (i in this)
                action.call(that, this[i], i, this);
    };
}
if (!('map' in Array.prototype)) {
    Array.prototype.map= function(mapper, that /*opt*/) {
        var other= new Array(this.length);
        for (var i= 0, n= this.length; i<n; i++)
            if (i in this)
                other[i]= mapper.call(that, this[i], i, this);
        return other;
    };
}
if (!('filter' in Array.prototype)) {
    Array.prototype.filter= function(filter, that /*opt*/) {
        var other= [], v;
        for (var i=0, n= this.length; i<n; i++)
            if (i in this && filter.call(that, v= this[i], i, this))
                other.push(v);
        return other;
    };
}
if (!('every' in Array.prototype)) {
    Array.prototype.every= function(tester, that /*opt*/) {
        for (var i= 0, n= this.length; i<n; i++)
            if (i in this && !tester.call(that, this[i], i, this))
                return false;
        return true;
    };
}
if (!('some' in Array.prototype)) {
    Array.prototype.some= function(tester, that /*opt*/) {
        for (var i= 0, n= this.length; i<n; i++)
            if (i in this && tester.call(that, this[i], i, this))
                return true;
        return false;
    };
}// # Recline Backbone Models
this.recline = this.recline || {};
this.recline.Model = this.recline.Model || {};

(function foo08(my) {

// use either jQuery or Underscore Deferred depending on what is available
var Deferred = _.isUndefined(this.jQuery) ? _.Deferred : jQuery.Deferred;

// ## <a id="dataset">Dataset</a>
my.Dataset = Backbone.Model.extend({
  constructor: function Dataset() {
    Backbone.Model.prototype.constructor.apply(this, arguments);
  },

  // ### initialize
  initialize: function() {
    _.bindAll(this, 'query');
    this.backend = null;
    if (this.get('backend')) {
      this.backend = this._backendFromString(this.get('backend'));
    } else { // try to guess backend ...
      if (this.get('records')) {
        this.backend = recline.Backend.Memory;
      }
    }
    this.fields = new my.FieldList();
    this.records = new my.RecordList();
    this._changes = {
      deletes: [],
      updates: [],
      creates: []
    };
    this.facets = new my.FacetList();
    this.recordCount = null;
    this.queryState = new my.Query();
    this.queryState.bind('change', this.query);
    this.queryState.bind('facet:add', this.query);
    // store is what we query and save against
    // store will either be the backend or be a memory store if Backend fetch
    // tells us to use memory store
    this._store = this.backend;
    if (this.backend == recline.Backend.Memory) {
      this.fetch();
    }
  },

  // ### fetch
  //
  // Retrieve dataset and (some) records from the backend.
  fetch: function() {
    var self = this;
    var dfd = new Deferred();

    if (this.backend !== recline.Backend.Memory) {
      this.backend.fetch(this.toJSON())
        .done(handleResults)
        .fail(function(args) {
          dfd.reject(args);
        });
    } else {
      // special case where we have been given data directly
      handleResults({
        records: this.get('records'),
        fields: this.get('fields'),
        useMemoryStore: true
      });
    }

    function handleResults(results) {
      var out = self._normalizeRecordsAndFields(results.records, results.fields);
      if (results.useMemoryStore) {
        self._store = new recline.Backend.Memory.Store(out.records, out.fields);
      }

      self.set(results.metadata);
      self.fields.reset(out.fields);
      self.query()
        .done(function() {
          dfd.resolve(self);
        })
        .fail(function(args) {
          dfd.reject(args);
        });
    }

    return dfd.promise();
  },

  // ### _normalizeRecordsAndFields
  // 
  // Get a proper set of fields and records from incoming set of fields and records either of which may be null or arrays or objects
  //
  // e.g. fields = ['a', 'b', 'c'] and records = [ [1,2,3] ] =>
  // fields = [ {id: a}, {id: b}, {id: c}], records = [ {a: 1}, {b: 2}, {c: 3}]
  _normalizeRecordsAndFields: function(records, fields) {
    // if no fields get them from records
    if (!fields && records && records.length > 0) {
      // records is array then fields is first row of records ...
      if (records[0] instanceof Array) {
        fields = records[0];
        records = records.slice(1);
      } else {
        fields = _.map(_.keys(records[0]), function(key) {
          return {id: key};
        });
      }
    } 

    // fields is an array of strings (i.e. list of field headings/ids)
    if (fields && fields.length > 0 && (fields[0] === null || typeof(fields[0]) != 'object')) {
      // Rename duplicate fieldIds as each field name needs to be
      // unique.
      var seen = {};
      fields = _.map(fields, function(field, index) {
        if (field === null) {
          field = '';
        } else {
          field = field.toString();
        }
        // cannot use trim as not supported by IE7
        var fieldId = field.replace(/^\s+|\s+$/g, '');
        if (fieldId === '') {
          fieldId = '_noname_';
          field = fieldId;
        }
        while (fieldId in seen) {
          seen[field] += 1;
          fieldId = field + seen[field];
        }
        if (!(field in seen)) {
          seen[field] = 0;
        }
        // TODO: decide whether to keep original name as label ...
        // return { id: fieldId, label: field || fieldId }
        return { id: fieldId };
      });
    }
    // records is provided as arrays so need to zip together with fields
    // NB: this requires you to have fields to match arrays
    if (records && records.length > 0 && records[0] instanceof Array) {
      records = _.map(records, function(doc) {
        var tmp = {};
        _.each(fields, function(field, idx) {
          tmp[field.id] = doc[idx];
        });
        return tmp;
      });
    }
    return {
      fields: fields,
      records: records
    };
  },

  save: function() {
    var self = this;
    // TODO: need to reset the changes ...
    return this._store.save(this._changes, this.toJSON());
  },

  transform: function(editFunc) {
    var self = this;
    if (!this._store.transform) {
      alert('Transform is not supported with this backend: ' + this.get('backend'));
      return;
    }
    this.trigger('recline:flash', {message: "Updating all visible docs. This could take a while...", persist: true, loader: true});
    this._store.transform(editFunc).done(function() {
      // reload data as records have changed
      self.query();
      self.trigger('recline:flash', {message: "Records updated successfully"});
    });
  },

  // ### query
  //
  // AJAX method with promise API to get records from the backend.
  //
  // It will query based on current query state (given by this.queryState)
  // updated by queryObj (if provided).
  //
  // Resulting RecordList are used to reset this.records and are
  // also returned.
  query: function(queryObj) {
    var self = this;
    var dfd = new Deferred();
    this.trigger('query:start');

    if (queryObj) {
      this.queryState.set(queryObj, {silent: true});
    }
    var actualQuery = this.queryState.toJSON();

    this._store.query(actualQuery, this.toJSON())
      .done(function(queryResult) {
        self._handleQueryResult(queryResult);
        self.trigger('query:done');
        dfd.resolve(self.records);
      })
      .fail(function(args) {
        self.trigger('query:fail', args);
        dfd.reject(args);
      });
    return dfd.promise();
  },

  _handleQueryResult: function(queryResult) {
    var self = this;
    self.recordCount = queryResult.total;
    var docs = _.map(queryResult.hits, function(hit) {
      var _doc = new my.Record(hit);
      _doc.fields = self.fields;
      _doc.bind('change', function(doc) {
        self._changes.updates.push(doc.toJSON());
      });
      _doc.bind('destroy', function(doc) {
        self._changes.deletes.push(doc.toJSON());
      });
      return _doc;
    });
    self.records.reset(docs);
    if (queryResult.facets) {
      var facets = _.map(queryResult.facets, function(facetResult, facetId) {
        facetResult.id = facetId;
        return new my.Facet(facetResult);
      });
      self.facets.reset(facets);
    }
  },

  toTemplateJSON: function() {
    var data = this.toJSON();
    data.recordCount = this.recordCount;
    data.fields = this.fields.toJSON();
    return data;
  },

  // ### getFieldsSummary
  //
  // Get a summary for each field in the form of a `Facet`.
  // 
  // @return null as this is async function. Provides deferred/promise interface.
  getFieldsSummary: function() {
    var self = this;
    var query = new my.Query();
    query.set({size: 0});
    this.fields.each(function(field) {
      query.addFacet(field.id);
    });
    var dfd = new Deferred();
    this._store.query(query.toJSON(), this.toJSON()).done(function(queryResult) {
      if (queryResult.facets) {
        _.each(queryResult.facets, function(facetResult, facetId) {
          facetResult.id = facetId;
          var facet = new my.Facet(facetResult);
          // TODO: probably want replace rather than reset (i.e. just replace the facet with this id)
          self.fields.get(facetId).facets.reset(facet);
        });
      }
      dfd.resolve(queryResult);
    });
    return dfd.promise();
  },

  // Deprecated (as of v0.5) - use record.summary()
  recordSummary: function(record) {
    return record.summary();
  },

  // ### _backendFromString(backendString)
  //
  // Look up a backend module from a backend string (look in recline.Backend)
  _backendFromString: function(backendString) {
    var backend = null;
    if (recline && recline.Backend) {
      _.each(_.keys(recline.Backend), function(name) {
        if (name.toLowerCase() === backendString.toLowerCase()) {
          backend = recline.Backend[name];
        }
      });
    }
    return backend;
  }
});


// ## <a id="record">A Record</a>
// 
// A single record (or row) in the dataset
my.Record = Backbone.Model.extend({
  constructor: function Record() {
    Backbone.Model.prototype.constructor.apply(this, arguments);
  },

  // ### initialize
  // 
  // Create a Record
  //
  // You usually will not do this directly but will have records created by
  // Dataset e.g. in query method
  //
  // Certain methods require presence of a fields attribute (identical to that on Dataset)
  initialize: function() {
    _.bindAll(this, 'getFieldValue');
  },

  // ### getFieldValue
  //
  // For the provided Field get the corresponding rendered computed data value
  // for this record.
  //
  // NB: if field is undefined a default '' value will be returned
  getFieldValue: function(field) {
    val = this.getFieldValueUnrendered(field);
    if (field && !_.isUndefined(field.renderer)) {
      val = field.renderer(val, field, this.toJSON());
    }
    return val;
  },

  // ### getFieldValueUnrendered
  //
  // For the provided Field get the corresponding computed data value
  // for this record.
  //
  // NB: if field is undefined a default '' value will be returned
  getFieldValueUnrendered: function(field) {
    if (!field) {
      return '';
    }
    var val = this.get(field.id);
    if (field.deriver) {
      val = field.deriver(val, field, this);
    }
    return val;
  },

  // ### summary
  //
  // Get a simple html summary of this record in form of key/value list
  summary: function(record) {
    var self = this;
    var html = '<div class="recline-record-summary">';
    this.fields.each(function(field) { 
      if (field.id != 'id') {
        html += '<div class="' + field.id + '"><strong>' + field.get('label') + '</strong>: ' + self.getFieldValue(field) + '</div>';
      }
    });
    html += '</div>';
    return html;
  },

  // Override Backbone save, fetch and destroy so they do nothing
  // Instead, Dataset object that created this Record should take care of
  // handling these changes (discovery will occur via event notifications)
  // WARNING: these will not persist *unless* you call save on Dataset
  fetch: function() {},
  save: function() {},
  destroy: function() { this.trigger('destroy', this); }
});


// ## A Backbone collection of Records
my.RecordList = Backbone.Collection.extend({
  constructor: function RecordList() {
    Backbone.Collection.prototype.constructor.apply(this, arguments);
  },
  model: my.Record
});


// ## <a id="field">A Field (aka Column) on a Dataset</a>
my.Field = Backbone.Model.extend({
  constructor: function Field() {
    Backbone.Model.prototype.constructor.apply(this, arguments);
  },
  // ### defaults - define default values
  defaults: {
    label: null,
    type: 'string',
    format: null,
    is_derived: false
  },
  // ### initialize
  //
  // @param {Object} data: standard Backbone model attributes
  //
  // @param {Object} options: renderer and/or deriver functions.
  initialize: function(data, options) {
    // if a hash not passed in the first argument throw error
    if ('0' in data) {
      throw new Error('Looks like you did not pass a proper hash with id to Field constructor');
    }
    if (this.attributes.label === null) {
      this.set({label: this.id});
    }

    if (this.attributes.type.toLowerCase() in this._typeMap) {
      this.attributes.type = this._typeMap[this.attributes.type.toLowerCase()];
    }
    if (options) {
      this.renderer = options.renderer;
      this.deriver = options.deriver;
    }
    if (!this.renderer) {
      this.renderer = this.defaultRenderers[this.get('type')];
    }
    
    
    this.facets = new my.FacetList();
  },
  _typeMap: {
    'text': 'string',
    'double': 'number',
    'float': 'number',
    'numeric': 'number',
    'int': 'integer',
    'datetime': 'date-time',
    'bool': 'boolean',
    'timestamp': 'date-time',
    'json': 'object'
  },
  defaultRenderers: {
    object: function(val, field, doc) {
      return JSON.stringify(val);
    },
    geo_point: function(val, field, doc) {
      return JSON.stringify(val);
    },
    'number': function(val, field, doc) {
      var format = field.get('format'); 
      if (format === 'percentage') {
        return val + '%';
      }else if(format === 'float-percentage') {
    	  return Math.round(val*100) + '%'
      }
      return val;
    },
    'string': function(val, field, doc) {
      var format = field.get('format');
      if (format === 'markdown') {
        if (typeof Showdown !== 'undefined') {
          var showdown = new Showdown.converter();
          out = showdown.makeHtml(val);
          return out;
        } else {
          return val;
        }
      } else if (format == 'plain') {
        return val;
      }else if (format === 'float') {
    	  if (val == Number(val) & val != ''){
    		  return parseFloat(val).toFixed(4);
    	  }else
    		  return val
      }else if(format === 'float-percentage') {
    	  if (!isNaN(val) && val.toString().indexOf('.') != -1 ||
    			  val == '1' || val == '0'){
    		  var color;
    		  if(parseFloat(val) > 0.7){
    			  color = 'green'
    		  }else if(parseFloat(val) > 0.3){
    			  color = 'orange'
    		  }else if (parseFloat(val) > 0){
    			  color = 'red'
    		  }else{
    			  color = 'grey'
    		  }
    		  return '<strong><font color="' + color+'">' + Math.round(val*100) + '%</font></strong>' 
    		  //return Math.round(val*100) + '%'
    	  }else{
    		  return val
    	  }
    	  
      }else {
        // as this is the default and default type is string may get things
        // here that are not actually strings
        if (val && typeof val === 'string') {
          val = val.replace(/(https?:\/\/[^ ]+)/g, '<a href="$1">$1</a>');
        }
        return val;
      }
    }
  }
});

my.FieldList = Backbone.Collection.extend({
  constructor: function FieldList() {
    Backbone.Collection.prototype.constructor.apply(this, arguments);
  },
  model: my.Field
});

// ## <a id="query">Query</a>
my.Query = Backbone.Model.extend({
  constructor: function Query() {
    Backbone.Model.prototype.constructor.apply(this, arguments);
  },
  defaults: function() {
    return {
      size: 100,
      from: 0,
      q: '',
      facets: {},
      filters: []
    };
  },
  _filterTemplates: {
    term: {
      type: 'term',
      // TODO do we need this attribute here?
      field: '',
      term: ''
    },
    range: {
      type: 'range',
      start: '',
      stop: ''
    },
    geo_distance: {
      type: 'geo_distance',
      distance: 10,
      unit: 'km',
      point: {
        lon: 0,
        lat: 0
      }
    }
  },  
  // ### addFilter(filter)
  //
  // Add a new filter specified by the filter hash and append to the list of filters
  //
  // @param filter an object specifying the filter - see _filterTemplates for examples. If only type is provided will generate a filter by cloning _filterTemplates
  addFilter: function(filter) {
    // crude deep copy
    var ourfilter = JSON.parse(JSON.stringify(filter));
    // not fully specified so use template and over-write
    if (_.keys(filter).length <= 3) {
      ourfilter = _.defaults(ourfilter, this._filterTemplates[filter.type]);
    }
    var filters = this.get('filters');
    filters.push(ourfilter);
    this.trigger('change:filters:new-blank');
  },
  updateFilter: function(index, value) {
  },
  // ### removeFilter
  //
  // Remove a filter from filters at index filterIndex
  removeFilter: function(filterIndex) {
    var filters = this.get('filters');
    filters.splice(filterIndex, 1);
    this.set({filters: filters});
    this.trigger('change');
  },
  // ### addFacet
  //
  // Add a Facet to this query
  //
  // See <http://www.elasticsearch.org/guide/reference/api/search/facets/>
  addFacet: function(fieldId) {
    var facets = this.get('facets');
    // Assume id and fieldId should be the same (TODO: this need not be true if we want to add two different type of facets on same field)
    if (_.contains(_.keys(facets), fieldId)) {
      return;
    }
    facets[fieldId] = {
      terms: { field: fieldId }
    };
    this.set({facets: facets}, {silent: true});
    this.trigger('facet:add', this);
  },
  addHistogramFacet: function(fieldId) {
    var facets = this.get('facets');
    facets[fieldId] = {
      date_histogram: {
        field: fieldId,
        interval: 'day'
      }
    };
    this.set({facets: facets}, {silent: true});
    this.trigger('facet:add', this);
  }
});


// ## <a id="facet">A Facet (Result)</a>
my.Facet = Backbone.Model.extend({
  constructor: function Facet() {
    Backbone.Model.prototype.constructor.apply(this, arguments);
  },
  defaults: function() {
    return {
      _type: 'terms',
      total: 0,
      other: 0,
      missing: 0,
      terms: []
    };
  }
});

// ## A Collection/List of Facets
my.FacetList = Backbone.Collection.extend({
  constructor: function FacetList() {
    Backbone.Collection.prototype.constructor.apply(this, arguments);
  },
  model: my.Facet
});

// ## Object State
//
// Convenience Backbone model for storing (configuration) state of objects like Views.
my.ObjectState = Backbone.Model.extend({
});


// ## Backbone.sync
//
// Override Backbone.sync to hand off to sync function in relevant backend
Backbone.sync = function(method, model, options) {
  return model.backend.sync(method, model, options);
};

}(this.recline.Model));

/*jshint multistr:true */


// Standard JS module setup
this.recline = this.recline || {};
this.recline.View = this.recline.View || {};

// Multi View
(function foo13($, my) {
// ## MultiView
//
// Manage multiple views together along with query editor etc. Usage:
// 
// <pre>
// var myExplorer = new model.recline.MultiView({
//   model: {{recline.Model.Dataset instance}}
//   el: {{an existing dom element}}
//   views: {{dataset views}}
//   state: {{state configuration -- see below}}
// });
// </pre> 
//
// ### Parameters
// 
// **model**: (required) recline.model.Dataset instance.
//
// **el**: (required) DOM element to bind to. NB: the element already
// being in the DOM is important for rendering of some subviews (e.g.
// Graph).
//
// **views**: (optional) the dataset views (Grid, Graph etc) for
// MultiView to show. This is an array of view hashes. If not provided
// initialize with (recline.View.)Grid, Graph, and Map views (with obvious id
// and labels!).
//
// <pre>
// var views = [
//   {
//     id: 'grid', // used for routing
//     label: 'Grid', // used for view switcher
//     view: new recline.View.Grid({
//       model: dataset
//     })
//   },
//   {
//     id: 'graph',
//     label: 'Graph',
//     view: new recline.View.Graph({
//       model: dataset
//     })
//   }
// ];
// </pre>
//
// **sidebarViews**: (optional) the sidebar views (Filters, Fields) for
// MultiView to show. This is an array of view hashes. If not provided
// initialize with (recline.View.)FilterEditor and Fields views (with obvious 
// id and labels!).
//
// <pre>
// var sidebarViews = [
//   {
//     id: 'filterEditor', // used for routing
//     label: 'Filters', // used for view switcher
//     view: new recline.View.FielterEditor({
//       model: dataset
//     })
//   },
//   {
//     id: 'fieldsView',
//     label: 'Fields',
//     view: new recline.View.Fields({
//       model: dataset
//     })
//   }
// ];
// </pre>
//
// **state**: standard state config for this view. This state is slightly
//  special as it includes config of many of the subviews.
//
// <pre>
// state = {
//     query: {dataset query state - see dataset.queryState object}
//     view-{id1}: {view-state for this view}
//     view-{id2}: {view-state for }
//     ...
//     // Explorer
//     currentView: id of current view (defaults to first view if not specified)
//     readOnly: (default: false) run in read-only mode
// }
// </pre>
//
// Note that at present we do *not* serialize information about the actual set
// of views in use -- e.g. those specified by the views argument -- but instead 
// expect either that the default views are fine or that the client to have
// initialized the MultiView with the relevant views themselves.
my.MultiView = Backbone.View.extend({
  template: ' \
  <div class="recline-data-explorer"> \
    <div class="alert-messages"></div> \
    \
    <div class="header clearfix"> \
      <div class="navigation"> \
        <div class="btn-group" data-toggle="buttons-radio"> \
        {{#views}} \
        <a href="#{{id}}" data-view="{{id}}" class="btn">{{label}}</a> \
        {{/views}} \
        </div> \
      </div> \
      <div class="recline-results-info"> \
        <span class="doc-count">{{recordCount}}</span> records\
      </div> \
      <div class="menu-right"> \
        <div class="btn-group" data-toggle="buttons-checkbox"> \
         <!-- {{#sidebarViews}} \
          <a href="#" data-action="{{id}}" class="btn">{{label}}</a> \
          {{/sidebarViews}} -->\
        </div> \
      </div> \
      <!-- <div class="query-editor-here" style="display:inline;"></div> -->\
    </div> \
    <div class="data-view-sidebar"></div> \
    <div class="data-view-container"></div> \
  </div> \
  ',
  events: {
    'click .menu-right a': '_onMenuClick',
    'click .navigation a': '_onSwitchView',
    //'click #translate-btn': '_onTranslateClick'
  },

  initialize: function(options) {
    var self = this;
    this.el = $(this.el);
    this._setupState(options.state);

    // Hash of 'page' views (i.e. those for whole page) keyed by page name
    if (options.views) {
      this.pageViews = options.views;
    } else {
      this.pageViews = [{
        id: 'grid',
        label: 'Grid',
        view: new my.SlickGrid({
          model: this.model,
          state: this.state.get('view-grid')
        })
      }, {
        id: 'graph',
        label: 'Graph',
        view: new my.Graph({
          model: this.model,
          state: this.state.get('view-graph')
        })
      }, {
        id: 'map',
        label: 'Map',
        view: new my.Map({
          model: this.model,
          state: this.state.get('view-map')
        })
      }, {
        id: 'timeline',
        label: 'Timeline',
        view: new my.Timeline({
          model: this.model,
          state: this.state.get('view-timeline')
        })
      }, {
        id: 'transform',
        label: 'Transform',
        view: new my.Transform({
          model: this.model
        })
      }];
    }
    // Hashes of sidebar elements
    if(options.sidebarViews) {
      this.sidebarViews = options.sidebarViews;
    } else {
      this.sidebarViews = [{
        id: 'filterEditor',
        label: 'Filters',
        view: new my.FilterEditor({
          model: this.model
        })
      }, {
        id: 'fieldsView',
        label: 'Fields',
        view: new my.Fields({
          model: this.model
        })
      }];
    }
    // these must be called after pageViews are created
    this.render();
    this._bindStateChanges();
    this._bindFlashNotifications();
    // now do updates based on state (need to come after render)
    if (this.state.get('readOnly')) {
      this.setReadOnly();
    }
    if (this.state.get('currentView')) {
      this.updateNav(this.state.get('currentView'));
    } else {
      this.updateNav(this.pageViews[0].id);
    }
    this._showHideSidebar();

    this.model.bind('query:start', function() {
        self.notify({loader: true, persist: true});
      });
    this.model.bind('query:done', function() {
        self.clearNotifications();
        self.el.find('.doc-count').text(self.model.recordCount || 'Unknown');
      });
    this.model.bind('query:fail', function(error) {
        self.clearNotifications();
        var msg = '';
        if (typeof(error) == 'string') {
          msg = error;
        } else if (typeof(error) == 'object') {
          if (error.title) {
            msg = error.title + ': ';
          }
          if (error.message) {
            msg += error.message;
          }
        } else {
          msg = 'There was an error querying the backend';
        }
        self.notify({message: msg, category: 'error', persist: true});
      });

    // retrieve basic data like fields etc
    // note this.model and dataset returned are the same
    // TODO: set query state ...?
    this.model.queryState.set(self.state.get('query'), {silent: true});
    this.model.fetch()
      .fail(function(error) {
        self.notify({message: error.message, category: 'error', persist: true});
      });
  },

  setReadOnly: function() {
    this.el.addClass('recline-read-only');
  },

  render: function() {
    var tmplData = this.model.toTemplateJSON();
    tmplData.views = this.pageViews;
    tmplData.sidebarViews = this.sidebarViews;
    var template = Mustache.render(this.template, tmplData);
    $(this.el).html(template);

    // now create and append other views
    var $dataViewContainer = this.el.find('.data-view-container');
    var $dataSidebar = this.el.find('.data-view-sidebar');

    
    // the main views
    _.each(this.pageViews, function(view, pageName) {
      view.view.render();
      $dataViewContainer.append(view.view.el);
      if (view.view.elSidebar) {
        $dataSidebar.append(view.view.elSidebar);
      }
    });

    _.each(this.sidebarViews, function(view) {
      this['$'+view.id] = view.view.el;
      $dataSidebar.append(view.view.el);
    }, this);

    var pager = new recline.View.Pager({
      model: this.model.queryState
    });
    this.el.find('.recline-results-info').after(pager.el);

    var queryEditor = new recline.View.QueryEditor({
      model: this.model.queryState
    });
    this.el.find('.query-editor-here').append(queryEditor.el);

  },

  // hide the sidebar if empty
  _showHideSidebar: function() {
    var $dataSidebar = this.el.find('.data-view-sidebar');
    var visibleChildren = $dataSidebar.children().filter(function() {
      return $(this).css("display") != "none";
    }).length;

    if (visibleChildren > 0) {
      $dataSidebar.show();
    } else {
      $dataSidebar.hide();
    }
  },
  updateNav: function(pageName) {
    this.el.find('.navigation a').removeClass('active');
    var $el = this.el.find('.navigation a[data-view="' + pageName + '"]');
    $el.addClass('active');

    // add/remove sidebars and hide inactive views
    _.each(this.pageViews, function(view, idx) {
      if (view.id === pageName) {
        view.view.el.show();
        if (view.view.elSidebar) {
          view.view.elSidebar.show();
        }
      } else {
        view.view.el.hide();
        if (view.view.elSidebar) {
          view.view.elSidebar.hide();
        }
        if (view.view.hide) {
          view.view.hide();
        }
      }
    });

    this._showHideSidebar();

    // call view.view.show after sidebar visibility has been determined so
    // that views can correctly calculate their maximum width
    _.each(this.pageViews, function(view, idx) {
      if (view.id === pageName) {
        if (view.view.show) {
          view.view.show();
        }
      }
    });
  },

  _onMenuClick: function(e) {
    e.preventDefault();
    var action = $(e.target).attr('data-action');
    this['$'+action].toggle();
    this._showHideSidebar();
  },

  _onSwitchView: function(e) {
    e.preventDefault();
    var viewName = $(e.target).attr('data-view');
    this.updateNav(viewName);
    this.state.set({currentView: viewName});
  },

  // create a state object for this view and do the job of
  // 
  // a) initializing it from both data passed in and other sources (e.g. hash url)
  //
  // b) ensure the state object is updated in responese to changes in subviews, query etc.
  _setupState: function(initialState) {
    var self = this;
    // get data from the query string / hash url plus some defaults
    var qs = my.parseHashQueryString();
    var query = qs.reclineQuery;
    query = query ? JSON.parse(query) : self.model.queryState.toJSON();
    // backwards compatability (now named view-graph but was named graph)
    var graphState = qs['view-graph'] || qs.graph;
    graphState = graphState ? JSON.parse(graphState) : {};

    // now get default data + hash url plus initial state and initial our state object with it
    var stateData = _.extend({
        query: query,
        'view-graph': graphState,
        backend: this.model.backend.__type__,
        url: this.model.get('url'),
        dataset: this.model.toJSON(),
        currentView: null,
        readOnly: false
      },
      initialState);
    this.state = new recline.Model.ObjectState(stateData);
  },

  _bindStateChanges: function() {
    var self = this;
    // finally ensure we update our state object when state of sub-object changes so that state is always up to date
    this.model.queryState.bind('change', function() {
      self.state.set({query: self.model.queryState.toJSON()});
    });
    _.each(this.pageViews, function(pageView) {
      if (pageView.view.state && pageView.view.state.bind) {
        var update = {};
        update['view-' + pageView.id] = pageView.view.state.toJSON();
        self.state.set(update);
        pageView.view.state.bind('change', function() {
          var update = {};
          update['view-' + pageView.id] = pageView.view.state.toJSON();
          // had problems where change not being triggered for e.g. grid view so let's do it explicitly
          self.state.set(update, {silent: true});
          self.state.trigger('change');
        });
      }
    });
  },

  _bindFlashNotifications: function() {
    var self = this;
    _.each(this.pageViews, function(pageView) {
      pageView.view.bind('recline:flash', function(flash) {
        self.notify(flash);
      });
    });
  },

  // ### notify
  //
  // Create a notification (a div.alert in div.alert-messsages) using provided
  // flash object. Flash attributes (all are optional):
  //
  // * message: message to show.
  // * category: warning (default), success, error
  // * persist: if true alert is persistent, o/w hidden after 3s (default = false)
  // * loader: if true show loading spinner
  notify: function(flash) {
    var tmplData = _.extend({
      message: 'Loading',
      category: 'warning',
      loader: false
      },
      flash
    );
    var _template;
    if (tmplData.loader) {
      _template = ' \
        <div class="alert alert-info alert-loader"> \
          {{message}} \
          <span class="notification-loader">&nbsp;</span> \
        </div>';
    } else {
      _template = ' \
        <div class="alert alert-{{category}} fade in" data-alert="alert"><a class="close" data-dismiss="alert" href="#">×</a> \
          {{message}} \
        </div>';
    }
    var _templated = $(Mustache.render(_template, tmplData));
    _templated = $(_templated).appendTo($('.recline-data-explorer .alert-messages'));
    if (!flash.persist) {
      setTimeout(function() {
        $(_templated).fadeOut(1000, function() {
          $(this).remove();
        });
      }, 1000);
    }
  },

  // ### clearNotifications
  //
  // Clear all existing notifications
  clearNotifications: function() {
    var $notifications = $('.recline-data-explorer .alert-messages .alert');
    $notifications.fadeOut(1500, function() {
      $(this).remove();
    });
  }
});

// ### MultiView.restore
//
// Restore a MultiView instance from a serialized state including the associated dataset
//
// This inverts the state serialization process in Multiview
my.MultiView.restore = function(state) {
  // hack-y - restoring a memory dataset does not mean much ... (but useful for testing!)
  var datasetInfo;
  if (state.backend === 'memory') {
    datasetInfo = {
      backend: 'memory',
      records: [{stub: 'this is a stub dataset because we do not restore memory datasets'}]
    };
  } else {
    datasetInfo = _.extend({
        url: state.url,
        backend: state.backend
      },
      state.dataset
    );
  }
  var dataset = new recline.Model.Dataset(datasetInfo);
  var explorer = new my.MultiView({
    model: dataset,
    state: state
  });
  return explorer;
};

// ## Miscellaneous Utilities
var urlPathRegex = /^([^?]+)(\?.*)?/;

// Parse the Hash section of a URL into path and query string
my.parseHashUrl = function(hashUrl) {
  var parsed = urlPathRegex.exec(hashUrl);
  if (parsed === null) {
    return {};
  } else {
    return {
      path: parsed[1],
      query: parsed[2] || ''
    };
  }
};

// Parse a URL query string (?xyz=abc...) into a dictionary.
my.parseQueryString = function(q) {
  if (!q) {
    return {};
  }
  var urlParams = {},
    e, d = function (s) {
      return unescape(s.replace(/\+/g, " "));
    },
    r = /([^&=]+)=?([^&]*)/g;

  if (q && q.length && q[0] === '?') {
    q = q.slice(1);
  }
  while (e = r.exec(q)) {
    // TODO: have values be array as query string allow repetition of keys
    urlParams[d(e[1])] = d(e[2]);
  }
  return urlParams;
};

// Parse the query string out of the URL hash
my.parseHashQueryString = function() {
  q = my.parseHashUrl(window.location.hash).query;
  return my.parseQueryString(q);
};

// Compse a Query String
my.composeQueryString = function(queryParams) {
  var queryString = '?';
  var items = [];
  $.each(queryParams, function(key, value) {
    if (typeof(value) === 'object') {
      value = JSON.stringify(value);
    }
    items.push(key + '=' + encodeURIComponent(value));
  });
  queryString += items.join('&');
  return queryString;
};

my.getNewHashForQueryString = function(queryParams) {
  var queryPart = my.composeQueryString(queryParams);
  if (window.location.hash) {
    // slice(1) to remove # at start
    return window.location.hash.split('?')[0].slice(1) + queryPart;
  } else {
    return queryPart;
  }
};

my.setHashQueryString = function(queryParams) {
  window.location.hash = my.getNewHashForQueryString(queryParams);
};

})(jQuery, recline.View);

/*jshint multistr:true */
/* HELLO CHANGES */

this.recline = this.recline || {};
this.recline.View = this.recline.View || {};

// SlickGrid View
(function foo14($, my) {
// ## SlickGrid Dataset View
//
// Provides a tabular view on a Dataset, based on SlickGrid.
//
// https://github.com/mleibman/SlickGrid
//
// Initialize it with a `recline.Model.Dataset`.
//
// Additional options to drive SlickGrid grid can be given through state.
// The following keys allow for customization:
// * gridOptions: to add options at grid level
// * columnsEditor: to add editor for editable columns
//
// For example:
//    var grid = new recline.View.SlickGrid({
//         model: dataset,
//         el: $el,
//         state: {
//          gridOptions: {editable: true},
//          columnsEditor: [
//            {column: 'date', editor: Slick.Editors.Date },
//            {column: 'title', editor: Slick.Editors.Text}
//          ]
//        }
//      });
//// NB: you need an explicit height on the element for slickgrid to work
my.SlickGrid = Backbone.View.extend({
  initialize: function(modelEtc) {
    var self = this;
    this.el = $(this.el);
    this.el.addClass('recline-slickgrid');
    _.bindAll(this, 'render');
    
    this.model.records.bind('add', this.render);
    this.model.records.bind('reset', this.render);
    this.model.records.bind('remove', this.render);
    this.model.records.bind('change', this.onRecordChanged, this);
    var state = _.extend({
        hiddenColumns: [],
        columnsOrder: [],
        columnsSort: {},
        columnsWidth: [],
        columnsEditor: [],
        options: {},
        fitColumns: false
      }, modelEtc.state

    );
    this.state = new recline.Model.ObjectState(state);
  },

  onRecordChanged: function(record) {
    // Ignore if the grid is not yet drawn
    if (!this.grid) {
      return;
    }
    // Let's find the row corresponding to the index
    var row_index = this.grid.getData().getModelRow( record );
    this.grid.invalidateRow(row_index);
    this.grid.getData().updateItem(record, row_index);
    this.grid.render();
  },

  render: function() {
    var self = this;
    var options = _.extend({
      enableCellNavigation: true,
      enableColumnReorder: true,
      explicitInitialization: true,
      syncColumnCellResize: true,
      //editable: true,
      autoEdit: false,
      //forceFitColumns: this.state.get('fitColumns')
      //forceFitColumns: true
    }, self.state.get('gridOptions'));
    // We need all columns, even the hidden ones, to show on the column picker
    var columns = [];
    // custom formatter as default one escapes html
    // plus this way we distinguish between rendering/formatting and computed value (so e.g. sort still works ...)
    // row = row index, cell = cell index, value = value, columnDef = column definition, dataContext = full row values
    var formatter = function(row, cell, value, columnDef, dataContext) {
      var field = self.model.fields.get(columnDef.id);

      if (field.renderer) {
        return field.renderer(value, field, dataContext);
      } else {
        return value;
      }
    };
    
    // Hiding columns which are assisting iterlinking (e.g. *_int-results). 
    // To do so these fields are included to the state.hiddenColumns[] array
    var hiddenColumns = []
    
    for(var i=0; i < this.model.fields.length; i++){  
    	if(this.model.fields.at(i).get("hostsAllInterlinkingResults") === true)
    		hiddenColumns.push(this.model.fields.at(i).id);
    }
    hiddenColumns.concat(self.state.get('hiddenColumns'));
    hiddenColumns = int_helper.uniquesArray(hiddenColumns); 
    self.state.set('hiddenColumns',hiddenColumns);   
    
    
    _.each(this.model.fields.toJSON(),function(field){
      
      //Define field formatter	
      var column = {
        id: field.id,
        name: field.label,
        field: field.id,
        sortable: true,
        resizable: true,
        rerenderOnResize: true,
        minWidth: 0,
        width: 80,
        formatter: formatter,
        //state: field.state,
        defaultSortAsc: true,
        //editor: Slick.Editors.Text  
      };

      if (field.hostsInterlinkingResult === true)
    	  column.cssClass = "interlinkingResult";
      else if (field.hostsInterlinkingScore === true){
    	  column.cssClass = "interlinkingScore";
    	  column.width = 50;
      }
      else if (field.hostsInterlinkingAuxField === true)
    	  column.cssClass = "InterlinkingAuxField";
      else if (field.hostsInterlinkinCheckedFlag === true){
    	  column.cssClass = "InterlinkinCheckedFlag";
    	  column.width = 40;	  
      }
      
      if (field.type == 'boolean'){
    	  column['formatter'] = function (row, cell, value, columnDef, dataContext) {
  		    return value ? "<img src='/img/tick.png'>" : "";
		  }
      }
      
        
      var widthInfo = _.find(self.state.get('columnsWidth'),function(c){return c.column === field.id;});
      if (widthInfo){
        column.width = widthInfo.width;
      }

      var editInfo = _.find(self.state.get('columnsEditor'),function(c){return c.column === field.id;});
      if (editInfo){
        column.editor = editInfo.editor;
      }
      columns.push(column);      
      
    });
    

    // Restrict the visible columns
    var visibleColumns = columns.filter(function(column) {
      return _.indexOf(self.state.get('hiddenColumns'), column.id) === -1;
    });

    
    // Order them if there is ordering info on the state
    if (this.state.get('columnsOrder') && this.state.get('columnsOrder').length > 0) {
      visibleColumns = visibleColumns.sort(function(a,b){
        return _.indexOf(self.state.get('columnsOrder'),a.id) > _.indexOf(self.state.get('columnsOrder'),b.id) ? 1 : -1;
      });
      columns = columns.sort(function(a,b){
        return _.indexOf(self.state.get('columnsOrder'),a.id) > _.indexOf(self.state.get('columnsOrder'),b.id) ? 1 : -1;
      });
    }
    
    /*
    // Move hidden columns to the end, so they appear at the bottom of the
    // column picker
    //TOCHECK: Maybe remove this part 
    var tempHiddenColumns = [];
    for (var i = columns.length -1; i >= 0; i--){
      if (_.indexOf(_.pluck(visibleColumns,'id'),columns[i].id) === -1){
        tempHiddenColumns.push(columns.splice(i,1)[0]);
      }
    }
    columns = columns.concat(tempHiddenColumns);
	*/
    
    // Transform a model object into a row
    function toRow(m) {
      var row = {};
      self.model.fields.each(function(field){
        row[field.id] = m.getFieldValueUnrendered(field);
      });

      return row;
    }

    function RowSet() {
      var models = [];
      var rows = [];

      this.push = function(model, row) {
        models.push(model);
        rows.push(row);
      };

      this.getLength = function() {return rows.length; };
      this.getItem = function(index) {return rows[index];};
      this.getItemMetadata = function(index) {return {};};
      this.getModel = function(index) {return models[index];};
      this.getModelRow = function(m) {return models.indexOf(m);};
      this.updateItem = function(m,i) {
        //rows[i] = toRow(m, m);
        rows[i] = toRow(m);
        models[i] = m;
      };
    }

    var data = new RowSet();

    this.model.records.each(function(doc){
        data.push(doc, toRow(doc));
    });


    this.grid = new Slick.Grid(this.el, data, visibleColumns, options);

    // Column sorting
    var sortInfo = this.model.queryState.get('sort');
    if (sortInfo){
      var column = sortInfo[0].field;
      var sortAsc = sortInfo[0].order !== 'desc';
      this.grid.setSortColumn(column, sortAsc);
    }

    this.grid.onSort.subscribe(function(e, args){
      var order = (args.sortAsc) ? 'asc':'desc';
      var sort = [{
        field: args.sortCol.field,
        order: order
      }];
      self.model.query({sort: sort});
    });

    this.grid.onColumnsReordered.subscribe(function(e, args){
      self.state.set({columnsOrder: _.pluck(self.grid.getColumns(),'id')});
    });

    this.grid.onColumnsResized.subscribe(function(e, args){
        var columns = args.grid.getColumns();
        var defaultColumnWidth = args.grid.getOptions().defaultColumnWidth;
        var columnsWidth = [];
        _.each(columns,function(column){
          if (column.width != defaultColumnWidth){
            columnsWidth.push({column:column.id,width:column.width});
          }
        });
        self.state.set({columnsWidth:columnsWidth});
    });

    this.grid.onCellChange.subscribe(function (e, args) {
      // We need to change the model associated value
      //
      var grid = args.grid;
      var model = data.getModel(args.row);

      var field = grid.getColumns()[args.cell].id;
      var v = {};
      v[field] = args.item[field];
      model.set(v);
    });

    var columnpicker = new Slick.Controls.IColumnPicker(this.model, columns, this.grid,
                                                       _.extend(options,{state:this.state}));
            
    if (self.visible){
      self.grid.init();
      self.rendered = true;
    } else {
      // Defer rendering until the view is visible
      self.rendered = false;
    }

    return this;
 },

  show: function() {
    // If the div is hidden, SlickGrid will calculate wrongly some
    // sizes so we must render it explicitly when the view is visible
    if (!this.rendered){
      if (!this.grid){
        this.render();
      }
      this.grid.init();
      this.rendered = true;
    }
    this.visible = true;
  },

  hide: function() {
    this.visible = false;
  }
});

})(jQuery, recline.View);

// InterlinkingColumnPicker
(function foo15($) {
  function InterlinkingColumnPicker(model, columns, grid, options) {
    var $menu;
    var column;
    var similarityResults = {};
    var selectedColumnIndex;
    var selectedField;
    
    var defaults = {
      fadeSpeed:250
    };
    
       
    function init() {
      grid.onHeaderContextMenu.subscribe(handleHeaderContextMenu);
      grid.onClick.subscribe(handleCellClick);
      options = $.extend({}, defaults, options);
      
      //TODO: Make sure that it is not created twice
      $menu = $('<ul id="interlinkingChoices" class="contextMenu" style="display:none;position:absolute; z-index:1" />').appendTo(document.body);
	  
      $menu.bind('mouseleave', function (e) {
        $(this).fadeOut(options.fadeSpeed);
      });
	      
      $menu.bind('click', updateColumn);
      
      $("#termsMenu")
  		.bind('mouseleave', function (e) {
  			$("#termsMenu").data("inMatchingTermsMenu", false);
  			setTimeout(function () {
  				if(!$("#termsMenu").data("inMatchingTermsMenu") ||
  						typeof $("#termsMenu").data("inMatchingTermsMenu") == "undefined"){
  	  				$("#termsMenu").fadeOut(options.fadeSpeed);  	
  		  			$("#matchingTermsMenu").fadeOut(options.fadeSpeed);
  	  			}
  		    }, 100);
  			
         })
	     .bind('mouseenter', function (e) {
	    	 $("#termsMenu").data("inMatchingTermsMenu", true);
         });
      
      $("#matchingTermsMenu")
      .bind("mouseenter", function(e){
    	  $("#termsMenu").data("inMatchingTermsMenu", true);
      })
      .bind('mouseleave', function(e){
    	  $("#termsMenu").data("inMatchingTermsMenu", false);
    	  $(this).fadeOut(options.fadeSpeed);
    	  
    	  setTimeout(function () {
    		  if(!$("#termsMenu").data('inMatchingTermsMenu')){
	  				$("#termsMenu").fadeOut(options.fadeSpeed);  	
	  			}
		    }, 200);
      });
      
    }
    
    function _onCompleteGetInterlinkingReferences(results){
        var results = results.responseJSON.result
        $('<b>' + interlinking_utility.i18n['interlinkWith'] + '</b>').appendTo($menu)
        for (var res in results){
        	ref = results[res]
        	
        	$li = $('<li />').appendTo($menu);
        	$li.attr({'id': ref['ref-id']}).text(ref['name']).data({'option': 'interlink-with', 'reference': ref['ref-id']})
        }
    }
    
    //This function handles clicks on columns which contain interlinking results
    function handleCellClick(e, args){
        e.preventDefault();
        
        selctedCell = args;
    	var fields = model.fields;
        var fieldID = grid.getColumns()[selctedCell.cell].field;
        selectedField = model.fields.get(fieldID); 
                
    	if(selectedField.get("hostsInterlinkingResult")){
    		// A context menu is loaded containing other alternatives    		
    		var originalFieldId = fields.at(fields.indexOf(selectedField) - 1).id;
    		var scoreFieldId = fields.at(fields.indexOf(selectedField) + 1).id;
    		var checkFieldId = fields.at(fields.indexOf(selectedField) + 2).id;
    		var resultsFieldId = fields.at(fields.indexOf(selectedField) + 3).id;
    		
            var ul = $("#termsMenu");
            ul.empty();
            var originalValue = model.records.at(selctedCell.row).get(originalFieldId)   
            var otherResults = JSON.parse(model.records.at(selctedCell.row).get(resultsFieldId)).records;

            var otherFields = JSON.parse(model.records.at(selctedCell.row).get(resultsFieldId)).fields;
            
            //sorting results
            otherResults.sort(_compareInterlinkingResults)
            
            // Creating options context menu
            ul.append('<b>'+ interlinking_utility.i18n['interlinkChoices'] +'</b>')

            for (var i=0; i < otherResults.length; i++){
            	var ul_text = '<li id="termOption"';
            	ul_text += ' term="' + otherResults[i][otherFields[0]] + '"';
            	ul_text += ' score="' + otherResults[i][otherFields[1]] + '"';
            	
            	// Dealing with interlinking auxiliary fields
            	for(var j=2; j< otherFields.length; j++){
            		ul_text += ' ' + otherFields[j] + '="' + otherResults[i][otherFields[j]] + '"';
            	}
            	            	
            	if(otherResults[i][otherFields[1]] == Number(otherResults[i][otherFields[1]]))
            		var score_part = parseFloat(otherResults[i][otherFields[1]]).toFixed(4)
            	else
            		var score_part = otherResults[i][otherFields[1]]
            	ul_text += '>' + otherResults[i][otherFields[0]] + "   (score: "+ score_part +")" + "</li>"
            	ul.append(ul_text);
            }
            if (model.records.at(selctedCell.row).get(checkFieldId))
            	ul.append('<hr /><li id="applyAllOption">' + interlinking_utility.i18n['applyAllMatchingTerms'] + '</li>');
            ul.append('<hr /><b>' + interlinking_utility.i18n['searchAnotherTerm'] + '</b>');
            ul.append('</br><input id="intSearchFld" class="search" type="text" autocomplete="off" placeholder="' + 
            		interlinking_utility.i18n['type3characters'] +'" value>');
            
            addTextAreaCallback( $( "#intSearchFld" )[0], function (){
				 var user_term = $("#intSearchFld").val();
				 var star_search_options  = {
							'term' :  user_term,
							'reference_resource': interlinking_utility.int_state['reference_resource']
		
						}
				 if (user_term.length >= 3)
					 int_helper.star_search(star_search_options, function() {}, _onCompleteFetchingStarSearchResults)
				 
            }, 1000 );
            
            console.log($("#termsMenu").height())
            var critical_difference = 575 - e.pageY - $("#termsMenu").height();
            if (critical_difference <= 0){
            	$("#termsMenu")
		        	.css("top", e.pageY -1 + critical_difference)
		        	.css("left", e.pageX -1)
		        	.show();
            }else{
	            $("#termsMenu")
		        	.css("top", e.pageY -1)
		        	.css("left", e.pageX -1)
		        	.show();
            }
            
            
                        
        	$("body").click(function(e) {
        	    if (!$(e.target).hasClass("slick-cell")){
        	    	$("#termsMenu").hide();
        	    	$("#matchingTermsMenu").hide();
        	    }
        	});
        	
    	} else{
            $("body").one("click", function () {
                $("#termsMenu").hide();
    	    	$("#matchingTermsMenu").hide();
             });
    	}
    }
    
    function _onCompleteFetchingStarSearchResults(results){
       	var fields = results.responseJSON.result.fields;
    	var hits = results.responseJSON.result.records;
    	var primaryField = fields[0];
    	var scoreField = fields[1];
    	var auxInterlinkFields = [];
    	for (var i=2; i < fields.length; i++){
    		auxInterlinkFields.push(fields[i]);
    	}

    	
    	 var ul_inner = $("#matchingTermsMenu");
		 ul_inner.empty();
		 for (var i = 0; i < hits.length; i++){
			 var ul_inner_text = '<li id="usersOption" term="' + hits[i][primaryField] + '"';
			 ul_inner_text += ' score="' + hits[i][scoreField] + '"';
			 for (j=0; j < auxInterlinkFields.length; j++){
				 var fieldname = interlinking_utility.int_state['fields_namespaced'] ? 
						 interlinking_utility.int_state['interlinking_resource_id'] + '.' + auxInterlinkFields[j] : 
						 auxInterlinkFields[j];

			     ul_inner_text += ' ' + fieldname.toLowerCase() + '="' + hits[i][auxInterlinkFields[j]] + '"';
			 }
			 ul_inner_text += '>'+ hits[i][primaryField] +'</li>';
			 ul_inner.append(ul_inner_text);
		 }
    	 if(hits.length > 0){
    		 var critical_difference = 575 - $("#termsMenu").position().top - $("#matchingTermsMenu").height();
    		 if (critical_difference <= 0){
             	console.log('problem')
             	$("#matchingTermsMenu")
 		        	.css("top", $("#termsMenu").offset().top + critical_difference)
 		        	.css("left", $("#termsMenu").offset().left + $("#termsMenu").width())
 		        	.show();
             }else{
             	console.log('ok')
 	            $("#matchingTermsMenu")
 		        	.css("top", $("#termsMenu").offset().top)
 		        	.css("left", $("#termsMenu").offset().left + $("#termsMenu").width())
 		        	.show();
             }
    		 /*
        	 $("#matchingTermsMenu")
	        	//.css("top", $("#termsMenu").offset().top + $("#termsMenu").height())
	        	//.css("left", $("#termsMenu").offset().left)
        	    .css("top", $("#termsMenu").offset().top)
        	    .css("left", $("#termsMenu").offset().left + $("#termsMenu").width())
	        	.show();
	        	*/
    	 }else{
    		 ;//$("#intSearchFld").attr()
    	 }
    }
    
    function addTextAreaCallback(textArea, callback, delay) {
	    var timer = null;
	    textArea.onkeyup = function() {
	    	if($("#intSearchFld").val().length >= 3){
		        if (timer) {
		            window.clearTimeout(timer);
		        }
		        timer = window.setTimeout( function() {
		            timer = null;
		            callback();
		        }, delay );
	    	}else{
	    		$("#matchingTermsMenu").hide();
	    	}
	    };
	    textArea = null;
	}
    

    function handleHeaderContextMenu(e, args) {
      e.preventDefault();
      
      if(e.target.id != ""){
          var selectedColumnHeader = e.target;
      }
      else{
          var selectedColumnHeader = $(e.target).parent();
      }
      
      column = args.column;

      var header = {}
      header.id = args.column.id;
      header.field = args.column.field;
      header.name = args.column.name
      
      selectedColumnIndex = grid.getColumnIndex(header.id)
      if(options.enableReOrderRow)
      	selectedColumnIndex--;
      if(options.enabledDelRow)
       	selectedColumnIndex--;
                
      selectedField = model.fields.get(grid.getColumns()[selectedColumnIndex]);
      
      // This context menu does not appear if allready a column is being interlinked
      if (typeof interlinking_utility.int_state['interlinked_column'] != 'undefined' &&
    		  typeof selectedField.get("hostsInterlinkingResult") == 'undefined')
    	  return
      
      // This context menu apperars only for ordinary columns
      if (	selectedField.get('hostsInterlinkingScore') === true ||
        		selectedField.get('hostsAllInterlinkingResults') === true ||
        		selectedField.get('hostsInterlinkinCheckedFlag') === true ||
        		selectedField.get('hostsInterlinkingAuxField') === true ||
        		selectedField.get('isInterlinked') === true ||
        		selectedField.get('id') === '_id'){
          return;
      } else if (selectedField.get("hostsInterlinkingResult") === true){
    	    // This is a context menu with two choices: abort-interlinking and finalize-interlinking 
    	    // It is reserved for a column which is under interlinking
	        origColumn = grid.getColumns()[selectedColumnIndex];
	        // set the grid's columns as the new columns
	        
	        // 1px is subtracted from X and Y axes in order to force the cursor to be inside this contextmenu 
	        // (instead of being placed exactly on the border). This allows the mouseleave event be triggered 
	        // right away without having the cursor having to enter the contextmenu first. 
	        $("#interlinkingHandling")
	           .css("top", e.pageY - 1) 
	           .css("left", e.pageX  - 26)
	           .show();
	        $("#interlinkingHandling")
	        	.bind('mouseleave', function (e) {
	        		$(this).fadeOut(options.fadeSpeed);
	          });
	        
	        $("#interlinkingHandling")
	        	.off('click').bind('click', updateColumn);
	
	        $("body").one("click", function () {
	          $("#interlinkingHandling").hide();
	        });

	        
      }else{
	      // Creating context menu for fields' heads
	      $menu.empty();
	      var $li, $input;
	      var interlinking_references = int_helper.get_interlinking_references(function() {}, _onCompleteGetInterlinkingReferences)
	      $menu.css('top', e.pageY -1)
	          .css('left', e.pageX - 26)
	          .fadeIn(options.fadeSpeed);
	        column = args.column;
      }
        
    }
    
    $("#termsMenu").off('click').click(function (e) {
    	if($(e.target).is("input"))
        	e.stopPropagation();
    	
    	if (!$(e.target).is("li")) {
            return;
        }  
    	if (!grid.getEditorLock().commitCurrentEdit()) {
            return;
        }
    	
    	var fields = model.fields;
    	var row = selctedCell.row;
    	var col = selctedCell.cell;
    	var record = Object.create(model.records.models[row]);
    	var idField = fields.at(0).id;
		var intFieldId = selectedField.id;
		var originalFieldId = fields.at(fields.indexOf(selectedField) - 1).id;
		var scoreFieldId = fields.at(fields.indexOf(selectedField) + 1).id;
		var checkedFieldId = fields.at(fields.indexOf(selectedField) + 2).id;
		var resultsFieldId = fields.at(fields.indexOf(selectedField) + 3).id;

		//Get all interlinking auxiliary fields
		var int_aux_fields = []
		for (var i=0; i < fields.length; i++){
			if (interlinking_utility.int_state['fields_status'][fields.at(i).id] == 'reference_auxiliary'){
				int_aux_fields.push(fields.at(i).id);
			}
		}
		
		var originalValue = model.records.at(selctedCell.row).get(originalFieldId)
		var selectedValue = model.records.at(selctedCell.row).get(intFieldId)
        var otherResults = JSON.parse(model.records.at(selctedCell.row).get(resultsFieldId))
        var row_id = model.records.at(selctedCell.row).get(idField)

    	if (e.target.id == "termOption"){
    		record.set(intFieldId, $(e.target).attr('term'))
    		record.set(scoreFieldId, $(e.target).attr('score'))
    		for(var i = 0; i< int_aux_fields.length; i++){
    			if (interlinking_utility.int_state['fields_namespaced'])
    				record.set(int_aux_fields[i],$(e.target).attr(int_aux_fields[i].split('.')[1]));
    			else
    				record.set(int_aux_fields[i],$(e.target).attr(int_aux_fields[i]));
    		}
    		record.set(checkedFieldId, true);
    		grid.getData().updateItem(record,row);
    		grid.updateRow(row);
    		grid.render();
    		dataExplorer.model.save();

    	} else if(e.target.id =="applyAllOption"){
    		model.trigger('applyToAll', row_id, originalValue, selectedValue);
    	}
		$("#termsMenu").hide();
		
    });
    
    $('#matchingTermsMenu').off('click').click(function (e) {
    	if($(e.target).is("input"))
        	e.stopPropagation();
    	
    	if (!$(e.target).is("li")) {
            return;
        }  
    	if (!grid.getEditorLock().commitCurrentEdit()) {
            return;
        }
    	
    	var fields = model.fields;
    	var row = selctedCell.row;
    	var col = selctedCell.cell;
    	var record = Object.create(model.records.models[row]);
		var intFieldId = selectedField.id;
		var originalFieldId = fields.at(fields.indexOf(selectedField) - 1).id;
		var scoreFieldId = fields.at(fields.indexOf(selectedField) + 1).id;
		var checkedFieldId = fields.at(fields.indexOf(selectedField) + 2).id;
		var resultsFieldId = fields.at(fields.indexOf(selectedField) + 3).id;

		var auxFieldIds = []
		for (var f in interlinking_utility.int_state['fields_status']){
			if (interlinking_utility.int_state['fields_status'][f] == 'reference_auxiliary')
				auxFieldIds.push(f);
		}
		var interlinkingResults = JSON.parse(record.get(resultsFieldId));
		var resultsFieldRecord = {};
		
		$.each(e.target.attributes, function(i, attrib){
			
		     var name = attrib.name;
		     var value = attrib.value;
		     
		     if (name === 'term'){
		    	 record.set(intFieldId, value);
		     	 resultsFieldRecord[interlinkingResults.fields[0]] = value;
		     }
		     else if (name === 'score'){
		    	 record.set(scoreFieldId, value);
		    	 resultsFieldRecord['scoreField'] = value;
		     }
		     else if (name !== 'id'){
		    	 for (var f in auxFieldIds){
		    		 if (auxFieldIds[f].toLowerCase() == name){
		    			 record.set(auxFieldIds[f], value);
		    			 if (interlinking_utility.int_state['fields_namespaced']){
		    				 resultsFieldRecord[auxFieldIds[f].split('.')[1]] = value;
		    			 }else
		    				 resultsFieldRecord[auxFieldIds[f]] = value;
		    			 break;
		    		 }
		    	 }

		     }
		  });
		if (_.pluck(interlinkingResults.records, interlinkingResults.fields[0]).indexOf(resultsFieldRecord[interlinkingResults.fields[0]])){
			interlinkingResults.records.push(resultsFieldRecord);
			record.set(resultsFieldId, JSON.stringify(interlinkingResults));
		}
		record.set(checkedFieldId, true);
    	grid.getData().updateItem(record,row);
    	grid.updateRow(row);
		grid.render();
		dataExplorer.model.save();
    });
    
    
    /*
    $("#termsMenu").bind('mouseleave', function (e) {
        $(this).fadeOut(options.fadeSpeed);
      });
	*/
    function updateColumn(e) {       
      if($(e.target).data('option') === 'interlink-with'){
    	  if (typeof $(e.target).data('reference') !== "undefined") {
    		  reference = $(e.target).data('reference')
    		  if (interlinking_utility.int_state['fields_namespaced'])
    			  col_id = column.id.split('.')[1];
    		  else
    			  col_id = column.id
    		  model.trigger('interlink-with', col_id, reference);
    	  }  
    	  
      }else if($(e.target).data('option') === 'finalize-interlinking'){
    	  model.trigger('finalize-interlinking');
    	  
      }else if($(e.target).data('option') === 'abort-interlinking'){
    	  model.trigger('abort-interlinking');
      }
    }
    
    function _compareInterlinkingResults(a, b){
    	return -(a.scoreField - b.scoreField)
    }
    init();
  }
  // Slick.Controls.ColumnPicker
    $.extend(true, window, { Slick:{ Controls:{ IColumnPicker:InterlinkingColumnPicker }}});
})(jQuery);


this.recline = this.recline || {};
this.recline.View = this.recline.View || {};

// Views module following classic module pattern
(function foo17($, my) {

// ## ColumnTransform
//
// View (Dialog) for doing data transformations
my.Transform = Backbone.View.extend({
  template: ' \
    <div class="recline-transform"> \
      <div class="script"> \
        <h2> \
          Transform Script \
          <button class="okButton btn btn-primary">Run on all records</button> \
        </h2> \
        <textarea class="expression-preview-code"></textarea> \
      </div> \
      <div class="expression-preview-parsing-status"> \
        No syntax error. \
      </div> \
      <div class="preview"> \
        <h3>Preview</h3> \
        <div class="expression-preview-container"></div> \
      </div> \
    </div> \
  ',

  events: {
    'click .okButton': 'onSubmit',
    'keydown .expression-preview-code': 'onEditorKeydown'
  },

  initialize: function(options) {
    this.el = $(this.el);
  },

  render: function() {
    var htmls = Mustache.render(this.template);
    this.el.html(htmls);
    // Put in the basic (identity) transform script
    // TODO: put this into the template?
    var editor = this.el.find('.expression-preview-code');
    if (this.model.fields.length > 0) {
      var col = this.model.fields.models[0].id;
    } else {
      var col = 'unknown';
    }
    editor.val("function(doc) {\n  doc['"+ col +"'] = doc['"+ col +"'];\n  return doc;\n}");
    editor.keydown();
  },

  onSubmit: function(e) {
    var self = this;
    var funcText = this.el.find('.expression-preview-code').val();
    var editFunc = recline.Data.Transform.evalFunction(funcText);
    if (editFunc.errorMessage) {
      this.trigger('recline:flash', {message: "Error with function! " + editFunc.errorMessage});
      return;
    }
    this.model.transform(editFunc);
  },

  editPreviewTemplate: ' \
      <table class="table table-condensed table-bordered before-after"> \
      <thead> \
      <tr> \
        <th>Field</th> \
        <th>Before</th> \
        <th>After</th> \
      </tr> \
      </thead> \
      <tbody> \
      {{#row}} \
      <tr> \
        <td> \
          {{field}} \
        </td> \
        <td class="before {{#different}}different{{/different}}"> \
          {{before}} \
        </td> \
        <td class="after {{#different}}different{{/different}}"> \
          {{after}} \
        </td> \
      </tr> \
      {{/row}} \
      </tbody> \
      </table> \
  ',

  onEditorKeydown: function(e) {
    var self = this;
    // if you don't setTimeout it won't grab the latest character if you call e.target.value
    window.setTimeout( function() {
      var errors = self.el.find('.expression-preview-parsing-status');
      var editFunc = recline.Data.Transform.evalFunction(e.target.value);
      if (!editFunc.errorMessage) {
        errors.text('No syntax error.');
        var docs = self.model.records.map(function(doc) {
          return doc.toJSON();
        });
        var previewData = recline.Data.Transform.previewTransform(docs, editFunc);
        var $el = self.el.find('.expression-preview-container');
        var fields = self.model.fields.toJSON();
        var rows = _.map(previewData.slice(0,4), function(row) {
          return _.map(fields, function(field) {
            return {
              field: field.id,
              before: row.before[field.id],
              after: row.after[field.id],
              different: !_.isEqual(row.before[field.id], row.after[field.id])
            }
          });
        });
        $el.html('');
        _.each(rows, function(row) {
          var templated = Mustache.render(self.editPreviewTemplate, {
            row: row
          });
          $el.append(templated);
        });
      } else {
        errors.text(editFunc.errorMessage);
      }
    }, 1, true);
  }
});

})(jQuery, recline.View);
/*jshint multistr:true */


// Field Info
//
// For each field
//
// Id / Label / type / format

// Editor -- to change type (and possibly format)
// Editor for show/hide ...

// Summaries of fields
//
// Top values / number empty
// If number: max, min average ...

// Box to boot transform editor ...



this.recline = this.recline || {};
this.recline.View = this.recline.View || {};

// Pager View
(function foo21($, my) {

my.Pager = Backbone.View.extend({
  className: 'recline-pager', 
  template: ' \
    <div class="pagination"> \
      <ul> \
        <li class="prev action-pagination-update"><a href="">&laquo;</a></li> \
        <li class="active"><a><input name="from" type="text" value="{{from}}" /> &ndash; <input name="to" type="text" value="{{to}}" /> </a></li> \
        <li class="next action-pagination-update"><a href="">&raquo;</a></li> \
      </ul> \
    </div> \
  ',

  events: {
    'click .action-pagination-update': 'onPaginationUpdate',
    'change input': 'onFormSubmit'
  },

  initialize: function() {
    _.bindAll(this, 'render');
    this.el = $(this.el);
    this.model.bind('change', this.render);
    this.render();
  },
  onFormSubmit: function(e) {
    e.preventDefault();
    var newFrom = parseInt(this.el.find('input[name="from"]').val());
    var newSize = parseInt(this.el.find('input[name="to"]').val()) - newFrom;
    newFrom = Math.max(newFrom, 0);
    newSize = Math.max(newSize, 1);
    this.model.set({size: newSize, from: newFrom});
    this.model.trigger('save');
  },
  onPaginationUpdate: function(e) {
    e.preventDefault();
    var $el = $(e.target);
    var newFrom = 0;
    if ($el.parent().hasClass('prev')) {
      newFrom = this.model.get('from') - Math.max(0, this.model.get('size'));
    } else {
      newFrom = this.model.get('from') + this.model.get('size');
    }
    newFrom = Math.max(newFrom, 0);
    this.model.set({from: newFrom});
    this.model.trigger('save');
  },
  render: function() {
    var tmplData = this.model.toJSON();
    tmplData.to = this.model.get('from') + this.model.get('size');
    var templated = Mustache.render(this.template, tmplData);
    this.el.html(templated);
  }
});

})(jQuery, recline.View);

/*jshint multistr:true */

this.recline = this.recline || {};
this.recline.View = this.recline.View || {};

// QueryEditor View
(function foo22($, my) {

my.QueryEditor = Backbone.View.extend({
  className: 'recline-query-editor', 
  template: ' \
    <form action="" method="GET" class="form-inline"> \
      <div class="input-prepend text-query"> \
        <span class="add-on"><i class="icon-search"></i></span> \
        <input type="text" name="q" value="{{q}}" class="span2" placeholder="Search data ..." class="search-query" /> \
      </div> \
      <button type="submit" class="btn">Go &raquo;</button> \
    </form> \
  ',

  events: {
    'submit form': 'onFormSubmit'
  },

  initialize: function() {
    _.bindAll(this, 'render');
    this.el = $(this.el);
    this.model.bind('change', this.render);
    this.render();
  },
  onFormSubmit: function(e) {
    e.preventDefault();
    var query = this.el.find('.text-query input').val();
    this.model.set({q: query});
  },
  render: function() {
    var tmplData = this.model.toJSON();
    var templated = Mustache.render(this.template, tmplData);
    this.el.html(templated);
  }
});

})(jQuery, recline.View);

/*jshint multistr:true */

this.recline = this.recline || {};
this.recline.View = this.recline.View || {};

// ValueFilter View
(function foo23($, my) {

my.ValueFilter = Backbone.View.extend({
  className: 'recline-filter-editor well', 
  template: ' \
    <div class="filters"> \
      <h3>Filters</h3> \
      <button class="btn js-add-filter add-filter">Add filter</button> \
      <form class="form-stacked js-add" style="display: none;"> \
        <fieldset> \
          <label>Field</label> \
          <select class="fields"> \
            {{#fields}} \
            <option value="{{id}}">{{label}}</option> \
            {{/fields}} \
          </select> \
          <button type="submit" class="btn">Add</button> \
        </fieldset> \
      </form> \
      <form class="form-stacked js-edit"> \
        {{#filters}} \
          {{{filterRender}}} \
        {{/filters}} \
        {{#filters.length}} \
        <button type="submit" class="btn update-filter">Update</button> \
        {{/filters.length}} \
      </form> \
    </div> \
  ',
  filterTemplates: {
    term: ' \
      <div class="filter-{{type}} filter"> \
        <fieldset> \
          {{field}} \
          <a class="js-remove-filter" href="#" title="Remove this filter" data-filter-id="{{id}}">&times;</a> \
          <input type="text" value="{{term}}" name="term" data-filter-field="{{field}}" data-filter-id="{{id}}" data-filter-type="{{type}}" /> \
        </fieldset> \
      </div> \
    '
  },
  events: {
    'click .js-remove-filter': 'onRemoveFilter',
    'click .js-add-filter': 'onAddFilterShow',
    'submit form.js-edit': 'onTermFiltersUpdate',
    'submit form.js-add': 'onAddFilter'
  },
  initialize: function() {
    this.el = $(this.el);
    _.bindAll(this, 'render');
    this.model.fields.bind('all', this.render);
    this.model.queryState.bind('change', this.render);
    this.model.queryState.bind('change:filters:new-blank', this.render);
    this.render();
  },
  render: function() {
    var self = this;
    var tmplData = $.extend(true, {}, this.model.queryState.toJSON());
    // we will use idx in list as the id ...
    tmplData.filters = _.map(tmplData.filters, function(filter, idx) {
      filter.id = idx;
      return filter;
    });
    tmplData.fields = this.model.fields.toJSON();
    tmplData.filterRender = function() {
      return Mustache.render(self.filterTemplates.term, this);
    };
    var out = Mustache.render(this.template, tmplData);
    this.el.html(out);
  },
  updateFilter: function(input) {
    var self = this;
    var filters = self.model.queryState.get('filters');
    var $input = $(input);
    var filterIndex = parseInt($input.attr('data-filter-id'), 10);
    var value = $input.val();
    filters[filterIndex].term = value;
  },
  onAddFilterShow: function(e) {
    e.preventDefault();
    var $target = $(e.target);
    $target.hide();
    this.el.find('form.js-add').show();
  },
  onAddFilter: function(e) {
    e.preventDefault();
    var $target = $(e.target);
    $target.hide();
    var field = $target.find('select.fields').val();
    this.model.queryState.addFilter({type: 'term', field: field});
  },
  onRemoveFilter: function(e) {
    e.preventDefault();
    var $target = $(e.target);
    var filterId = $target.attr('data-filter-id');
    this.model.queryState.removeFilter(filterId);
  },
  onTermFiltersUpdate: function(e) {
    var self = this;
    e.preventDefault();
    var filters = self.model.queryState.get('filters');
    var $form = $(e.target);
    _.each($form.find('input'), function(input) {
      self.updateFilter(input);
    });
    self.model.queryState.set({filters: filters, from: 0});
    self.model.queryState.trigger('change');
  }
});

})(jQuery, recline.View);
