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
        console.log(url)
        console.log(options)
        return this.call_ajax(url, options, ld, cb);           
    }; 

    this.delete = function(options, ld, cb) {
        var options = options || {};
        var col_name = options.column_id;
        //console.log(col_name)
        var self = this;

        var url = resource.endpoint + '/3/action/interlinking_resource_delete';
        
        var new_res_id = resource.temp_interlinking_resource;
        
        if (col_name !== undefined){
	        var options = {
	            resource_id: new_res_id,
	            column_name: col_name
	        }
        }
        else{
            var options = {
                resource_id: new_res_id
            }
        }
        
        console.log(url)
        console.log(options)
        return self.call_ajax(url, options, ld, cb);    
    };
    
    this.publish = function(options, ld, cb) {
        var url = resource.endpoint + '/3/action/interlinking_resource_finalize';
        var res_id = resource.id;

        var options = {
            resource_id:res_id,
        }
        console.log(url)
        console.log(options)   
        return self.call_ajax(url, options, ld, cb);
    };

    this.finalize = function(options, ld, cb) {
        var url = resource.endpoint + '/3/action/interlinking_resource_finalize';
        var res_id = resource.temp_interlinking_resource;
        var col_name = options.column_id;
        var return_url = options.return_url;

        var options = {
            resource_id: res_id,
            column_name: col_name,
        }
        return this.call_ajax(url, options, ld, cb);    
    };
    
    this.get_interlinking_references = function(ld, cb){
    	var url = resource.endpoint + '/3/action/interlinking_get_reference_resources';
    	options = {}
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

    this.show_resource =  function(resource, cb) {

    	var url = resource.endpoint + '/3/action/resource_show';
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
    
    this.call_ajax = function(url, options, ld, cb) {
    	//console.log('----------------------AJAX-----------------------')
    	//console.log(options)		
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
                //console.log('failed');
                //console.log(response);
            },
            error: function(response) {
                //if (response.status == 409){
                //    return;
                //}
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
                //console.log('failed');
                //console.log(response);
            },
            error: function(response) {
                //if (response.status == 409){
                //    return;
                //}
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




