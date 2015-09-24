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
        var col_name = options.column_name;
        var reference_resource = options.reference_resource;
        var self = this;
        var url = resource.endpoint + '/3/action/interlinking_resource_update';
                
        
        var new_res_id = resource.temp_interlinking_resource;
        
        //TOCHECK: Is res needed?
        var res = {endpoint:resource.endpoint, id:new_res_id};
        var options = {
                    resource_id: new_res_id,
                    column_name: col_name,
                    reference_resource: mode,
                }
        return this.call_ajax(url, options, ld, cb);           
    }; 

    this.delete = function(options, ld, cb) {
        var options = options || {};
        var col_name = options.column;

        var self = this;

        var url = resource.endpoint + '/3/action/interlinking_resource_delete';
        
        var new_res_id = resource.being_interlinked_with;
        
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
        return self.call_ajax(url, options, ld, cb);    
    };

    this.finalize = function(options, ld, cb) {
        var url = resource.endpoint + '/3/action/interlinking_resource_finalize';
        
        var res_id = resource.id;

        var options = {
            resource_id:res_id,
        }
        return this.call_ajax(url, options, ld, cb);    
    };


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
        console.log('options')
        console.log(options)
        
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
                //console.log('succeeded');
                //console.log(response);
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
    this._strip_package_id = function(url) {
        // CKAN 2.2 doesn't provide package_id in resource_show
        // strip it from url
        var str = "dataset/";
        var start = url.indexOf(str) + str.length;
        var str = "/resource";
        var end = url.indexOf(str);
        return url.substring(start, end);

    };
};




