// API calls helpers 
//
//this.TranslateApiHelper = this.TranslateApiHelper || {};

//(function ($, my) {
function InterlinkHelper (resource){
    this.resource;
    this.initialize = function (resource) {
        this.resource = resource;
    };

    this.create = function(ld, cb) {
        var url = resource.endpoint + '/3/action/interlinking_resource_create';
        var package_id = this._strip_package_id(resource.url);
        var options = {
            resource_id:resource.id,
            package_id: package_id,
        }
        return this.call_ajax(url, options, ld, cb);    
    };

    this.update = function(options, ld, cb) {
        var options = options || {};
        var col_name = options.column_id;
        var reference_resource = options.reference_resource;
        var self = this;
        var url = resource.endpoint + '/3/action/interlinking_resource_update';
                
        
        var new_res_id = resource.temp_interlinking_resource;
        
        //TOCHECK: Is res needed?
        var res = {endpoint:resource.endpoint, id:new_res_id};
        var options = {
                    resource_id: new_res_id,
                    column_name: col_name,
                    reference_resource: reference_resource
                }
        interlinking_utility.int_state['interlinked_column'] = col_name;
        return this.call_ajax(url, options, ld, cb);           
    }; 

    this.delete = function(options, ld, cb) {
        var options = options || {};
        var self = this;

        var url = resource.endpoint + '/3/action/interlinking_resource_delete';
        
        var new_res_id = resource.temp_interlinking_resource;
        var options = {
            resource_id: new_res_id
        }
       
        delete interlinking_utility.int_state['interlinked_column'];
        delete interlinking_utility.int_state['reference_resource'];
        return self.call_ajax(url, options, ld, cb);    
    };
   

    this.finalize = function(options, ld, cb) {
        var url = resource.endpoint + '/3/action/interlinking_resource_finalize';
        var res_id = resource.temp_interlinking_resource;
        var return_url = options.return_url;
        
        var package_id = this._strip_package_id(resource.url);

        var options = {
        	package_id: package_id,
        	resource_id: res_id,
        }
        delete interlinking_utility.int_state['interlinked_column'];
        delete interlinking_utility.int_state['reference_resource'];
        return this.call_ajax(url, options, ld, cb);    
    };
    
    this.get_interlinking_references = function(ld, cb){
    	var url = resource.endpoint + '/3/action/interlinking_get_reference_resources';
    	options = {}
    	return this.call_ajax(url, options, ld, cb);
    },
    
    this.check_interlink_complete = function(options, ld, cb){
    	var url = resource.endpoint + '/3/action/interlinking_check_interlink_complete';
    	var options = {
            	resource_id: options.resource_id,
                column_name: options.column_name,
            }
    	return this.call_ajax(url, options, ld, cb);
    },
    
    
    this.applyToAll = function(options, ld, cb){    	
    	var url = resource.endpoint + '/3/action/interlinking_apply_to_all';
    	var options = {
            	resource_id: options.resource_id,
            	row_id: options.row_id,
            }
    	return this.call_ajax(url, options, ld, cb);
    },
    
    
    this.star_search = function (options, ld, cb){
    	var url = resource.endpoint + '/3/action/interlinking_star_search';
    	options = {
    			term: options.term,
    			reference_resource: options.reference_resource, 
    	}
    	return this.call_ajax(url, options, ld, cb);
    },


    this.show =  function(resource, cb) {

        var url = resource.endpoint + '/3/action/datastore_search';
        var options = {
            id: resource.id,
        }
        return $.ajax({
                type: "POST",
                url: url,
                data: JSON.stringify(options),
                dataType: 'json',
                async: true, 
                complete: cb,
        });    
    },

    this.show_resource =  function(resource_id, cb) {
    	var url = resource.endpoint + '/3/action/resource_show';
        var options = {
            id: resource_id,
        }
        
        return $.ajax({
                type: "POST",
                url: url,
                data: JSON.stringify(options),
                dataType: 'json',
                async: true,
                complete: cb,
        });    
    },
    
    
    
    this.call_ajax = function(url, options, ld, cb) {	
        return $.ajax({
            type: "POST",
            url: url,
            data: JSON.stringify(options),
            dataType: 'json',
            async: true,
            beforeSend: ld,
            complete: cb,
            success: function(response) {
                return response;
            },
            failure: function(response) {

            },
            error: function(response) {
                console.log('error');
                console.log(response);
                alert('Error: .\n' + response.status + ':' + response.responseText);
            },
        });
    };
    
    this.call_get = function(url){
    	return $.post({
            url: url,
            dataType: 'json',
            success: function(response) {
                return response;
            },
            failure: function(response) {

            },
            error: function(response) {
                console.log('error');
                console.log(response);
                alert('Error: .\n' + response.status + ':' + response.responseText);
            },
        });
    }
    
    this._strip_package_id = function(url) {
        // CKAN 2.2 doesn't provide package_id in resource_show
        // strip it from url
        var str = "dataset/";
        var start = url.indexOf(str) + str.length;
        var str = "/resource";
        var end = url.indexOf(str);
        return url.substring(start, end);

    };
    
    // Given an array with duplicates, an array with unique values is returned
    this.uniquesArray = function (input) {
    	var u = {}, a = [];
    	for(var i = 0, l = input.length; i < l; ++i){
    		if(u.hasOwnProperty(input[i])) {
    			continue;
    	    }
    	    a.push(input[i]);
    	    u[input[i]] = 1;
    	}
    	return a;
    }
};




