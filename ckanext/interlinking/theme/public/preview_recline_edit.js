// recline preview module
var dataExplorer;

var errorMsg;

this.ckan.module('recline_interlink_preview', function (jQuery, _) {  
  return {
    options: {
      i18n: {
        heading: _("Please Confirm Action"),
        datastore_disabled: _("Datastore is disabled. Please enable datastore and try again in order to proceed with resource interlinking"),
        confirm_delete: _("Are you sure you want to restore this column to its status before interlinking take place?"),
        //confirm_update: _("Are you sure you want to update existing column interlinking?"),
        confirm_update: _("Are you sure you want to restart interlinking on this column?"),
        confirm_finalize: _("Finalizing the interlinking process for a column means that its contents will be update accordingly. Are you sure you want to finalize interlinking for this column?"),
        errorLoadingPreview: _("Could not load preview"),
        errorDataProxy: _("DataProxy returned an error"),
        errorDataStore: _("DataStore returned an error"),
        interlinkWith: _("Interlink with:"),
        previewNotAvailableForDataType: _("Preview not available for data type: ")
      },
    template: [
        '<div class="modal">',
        '<div class="modal-header">',
        '<a href="#" class="close" data-dismiss="modal">&times;</a>',
        '<h3>Tranlate Column Title</h3>',
        '</div>',
        '<div class="modal-body">',
        '<div class="divDialogElements">',
        '<label><h4>Column title:</h4></label>',
        '<input class="medium" id="xlInput" name="xlInput" type="text" />',
        '</div>',
        '</div>',
        '<div class="modal-footer">',
        '<a href="#" class="btn btn-cancel" id="closeDialog">Cancel</a>',
        '<a href="#" class="btn btn-primary" id="okClicked">OK</a>',
        '</div>',
        '</div>'
      ].join('\n'),

      site_url: ""
    },

    initialize: function () {
      //console.log('point 1')
      jQuery.proxyAll(this, /_on/);
      this.save_btn = jQuery("#saveClicked");
      this.publish_btn = jQuery("#publishClicked");
      this.finalize_btn = jQuery("#goResourcesClicked")
      this.el.ready(this._onReady);
    },
    _onReady: function() {
      //console.log('point 2')
      //console.log(preload_resource)
      //console.log(window)
      this.loadPreviewDialog(preload_resource);
      // Context menu `interlinkingHandling`'s options get a data attribute
      $('ul#interlinkingHandling > li#finalizeOption').data({'option': 'finalize-interlinking'})
      $('ul#interlinkingHandling > li#abortOption').data({'option': 'abort-interlinking'})      
    },

    // **Public: Loads a data preview**
    //
    // Fetches the preview data object from the link provided and loads the
    // parsed data from the webstore displaying it in the most appropriate
    // manner.
    //
    // link - Preview button.
    //
    // Returns nothing.
    loadPreviewDialog: function (resourceData) {
      var self = this;
      //console.log('point 3')
      function showError(msg){
        msg = msg || _('error loading preview');
        window.parent.ckan.pubsub.publish('data-viewer-error', msg);
      }

      recline.Backend.DataProxy.timeout = 10000;

      // 2 situations
      // a) something was posted to the datastore - need to check for this
      // b) csv or xls (but not datastore)
      resourceData.formatNormalized = this._normalizeFormat(resourceData.format);

      resourceData.url  = this._normalizeUrl(resourceData.url);
      if (resourceData.formatNormalized === '') {
        var tmp = resourceData.url.split('/');
        tmp = tmp[tmp.length - 1];
        tmp = tmp.split('?'); // query strings
        tmp = tmp[0];
        var ext = tmp.split('.');
        if (ext.length > 1) {
          resourceData.formatNormalized = ext[ext.length-1];
        }
      }
      var dataset; 

      if (resourceData.datastore_active) {
        resourceData.backend =  'ckanInterlinkEdit';
      	
        // Set endpoint of the resource to the datastore api (so it can locate
        // CKAN DataStore)
        //resourceData.endpoint = jQuery('body').data('site-root') + 'api';
        resourceData.endpoint = this.options.site_url + 'api';
         
        dataset = new recline.Model.Dataset(resourceData);
        errorMsg = this.options.i18n.errorLoadingPreview + ': ' + this.options.i18n.errorDataStore;
        
        int_helper = new InterlinkHelper(resourceData); 
        this.options.res_interlink_id = res_interlink_id = resourceData.temp_interlinking_resource;

        //if (this.options.res_interlink_id === undefined  && !(resourceData.on_interlinking_process == 'True')){
        /*
         * 
        console.log('resourceData on loadPreviewDialog')
        console.log(resourceData)
         */
        if(!(resourceData.on_interlinking_process == 'True')){
            //console.log("Create new");
            resourceData = int_helper.create(function() {}, function() { 
            	//console.log('resourceData on loadPreviewDialog Create New')
            	//console.log(resourceData)
            	self.initializeDataset(dataset, resourceData);
            });
            //Need reload in order to move on
            //Fix this
        }else{
            var translationResource = null;
            this.initializeDataset(dataset, resourceData);
        }
      }
      else{
          //TODO: doesn't work for some reason
          self.sandbox.notify(self.i18n('datastore_disabled'), 'error');
      }
    },
    
    _onCompleteShow: function(){
        //console.log('point 4.1')    
        var self = this;
        if (this.options.action == 'finalize'){
        	window.top.location.href = this.options.options.return_url;
        }else{
	        dataExplorer.model.fetch().done(function(dataset){
	        	//console.log('point 4.1.1')
	            var res = {id: self.options.res_interlink_id, endpoint: self.options.site_url + 'api'};
	            int_helper.show_resource(res, function(response){
	                if (response){
	                    var columns= {};
	                    try{
	                        //console.log(response.responresourceDataseJSON.result);
	                        columns = JSON.parse(response.responseJSON.result.interlinking_columns_status);
	                        self.options.columns = columns;
	                        //console.log(columns);
	                        //self._onRepaint(columns);
	                    }
	                    catch(err) {
	                        //console.log('point oops');
	                    }
	                }
	                else{
	                    //console.log('point resource fetch failed');
	                }
	            });
	        });
        }
    },
    
    _onCompleteGetInterlinkingReferences: function(results){
        //console.log('point 4.2')  
        var results = results.responseJSON.result
        for (var res in results){
        	ref = results[res]
        }
    },
    _onLoad: function(){
        //console.log('point 5')
        dataExplorer.notify({message: 'Loading', loader:true, category: 'warning', persist: true});
        //setTimeout(function(){ dataExplorer.model.fetch()}, 3000);
    },

    initializeDataset: function(dataset, resourceData) {
        //console.log('point 6')

        var self = this;
	    function showError(msg){
	        msg = msg || _('error loading preview');
	        window.parent.ckan.pubsub.publish('data-viewer-error', msg);
	    }
	    //console.log('point 6.1')
        dataset.fetch()
        	.done(function(dataset1){
                //console.log('point 6.2')
                var fields1 = dataset1.fields.models;
                var records1 = dataset1.records.models;
                
                self.initializeDataExplorer(dataset1);
                dataset.bind('interlink-with', function(col, reference_resource){
                    var options = {column_id: col.id, reference_resource: reference_resource};
                    self.updateWithConfirmation(dataset1, options);
                });              
                dataset.bind('finalize-interlinking', function(col){
                    var options = {column_id: col.id,
                    				return_url: resourceData.url.substring(0,resourceData.url.indexOf('download'))};
                    self.finalizeWithConfirmation(dataset1, options);
                });
                dataset.bind('abort-interlinking', function(col){
                    var options = {column_id: col.name};
                    self.deleteWithConfirmation(dataset1, options); 
                });

                dataset1.queryState.bind('save', function(){
                    self.sandbox.notify('hello', 'success');
                    //self.sandbox.client.favoriteDataset(this.button.val()).done(self._onSuccess);                  
                    dataset1.save();
                });

                self.save_btn.click(function() {
                    dataset1.save();
                });

                self.publish_btn.click(function() {
                    //TODO: Save before publishing - something doesnt work
                    //dataset.save().done(function(dataset){
                    //int_helper.publish(self._onLoad, function() { window.top.location.href = resourceData.url.substring(0,resourceData.url.indexOf('resource'))})
                    self.publishWithConfirmation(self._onLoad, function() { window.top.location.href = resourceData.url.substring(0,resourceData.url.indexOf('resource'))}); 
                    //});
                })
                self.finalize_btn.click(function(){

                })
        
          })
          .fail(function(error){
            if (error.message) errorMsg += ' (' + error.message + ')';
            showError(errorMsg);
          });
    },
    _onSuccess: function(e) {
        //console.log('point 7')
    },
    _onEditor: function(column) {
        //console.log('point 8')
        var pos = column.name.indexOf('-il');
        if (pos > -1){
            return  Slick.Editors.Text
        }
        else{
            return null;
        }
    },
    
    initializeDataExplorer: function (dataset) {
      //console.log('point 9')
      var views = [
        {
          id: 'grid',
          label: 'Grid',
          view: new recline.View.SlickGrid({
            model: dataset,
            state: { gridOptions: {editable:true, editorFactory: {getEditor:this._onEditor} } }
          })
        },        
      ];

      var sidebarViews = [
        {
          id: 'valueFilter',
          label: 'Filters',
          view: new recline.View.ValueFilter({
            model: dataset
          })
        }
      ];

      dataExplorer = new recline.View.MultiView({
        el: this.el,
        model: dataset,
        views: views,
        sidebarViews: sidebarViews,
        config: {
          readOnly: true,
        }
      });
    },
    
    _normalizeFormat: function (format) {
      //console.log('point 10')	
      var out = format.toLowerCase();
      out = out.split('/');
      out = out[out.length-1];
      return out;
    },
    _normalizeUrl: function (url) {
      //console.log('point 11')	
      if (url.indexOf('https') === 0) {
        return 'http' + url.slice(5);
      } else {  
        return url;
      }
    },
    deleteWithConfirmation: function(dataset, options, ld, cb) {
    	//console.log('point 12')	
            var ld = ld || this._onLoad;
            var cb = cb || this._onCompleteShow;
            var field_exists = this.checkFieldExists(dataset, options);
            if (field_exists){
                this.options.helper = int_helper;
                this.options.action = 'delete';
                this.options.options = options;
                this.options.cb = cb;
                this.options.ld = ld;
                this.confirm(this.i18n('confirm_delete'));
            }
            else{
            }        
    },
    
    finalizeWithConfirmation: function(dataset, options, ld, cb){
    	//console.log('point 13')	
        var ld = ld || this._onLoad;
        var cb = cb || this._onCompleteShow;
        var field_exists = this.checkFieldExists(dataset, options);
        if (field_exists){
            this.options.helper = int_helper;
            this.options.action = 'finalize';
            this.options.options = options;
            this.options.cb = cb;
            this.options.ld = ld;
            this.confirm(this.i18n('confirm_finalize'));
        }
        else{
        }       
    },
    
    publishWithConfirmation: function(ld, cp) {
    	//console.log('point 14')
    	var interlinking_references = int_helper.get_interlinking_references(function() {}, this._onCompleteGetInterlinkingReferences)
    	
    	/*
        var ld = ld || this._onLoad;
        var cb = cb || this._onCompleteShow;
        this.options.helper = int_helper;
        this.options.action = 'get_interlinking_references';
        this.options.options = {};
        this.options.cb = cb;
        this.options.ld = ld;
        this.confirm(this.i18n('confirm_publish'));
        */
    },
    
    // TOCHECK: Currently not used. Is it usefull to retain it?
    updateWithConfirmation: function(dataset, options, ld, cb) {
    	//console.log('point 15')
            var ld = ld || this._onLoad;
            var cb = cb || this._onCompleteShow;
            var field_exists = this.checkFieldExists(dataset, options);

            if (field_exists){
                this.options.helper = int_helper;
                this.options.action = 'update';
                this.options.options = options;
                this.options.ld = ld;
                this.options.cb = cb;
                this.confirm(this.i18n('confirm_update'));
            }
            else{
                int_helper.update(options, this._onLoad, this._onCompleteShow);
            }        
    },
    //It checks if a field contains interlinked results (best results and/or user choices)
    checkFieldExists: function(dataset, options){
    	//console.log('point 16')
    	// If such a column exists its field id has an '_int' suffix, and another one with tha same id 
    	// but without the '_int' ending exists as well
        var col = options.column_id;
    	col = col+'_int'
    	var col_suffix = col.substr(col.length-4, col.length-1)
    	var col_prefix = col.substr(0, col.length-4)
    	var field_exists = false
    	if(col_suffix === '_int'){
	        var fields = dataset.fields.models;
	        if(col.substr(col.length-3, col.length-1))
	        fields.forEach(function(fld, idx){
	            if (fld.id == col_prefix){
	            	return field_exists = true;
	            }
	        });
    	}
        return field_exists;
    },
    confirm: function (text) {
      //console.log('point 17')
      this.sandbox.body.append(this.createModal(text));
      this.modal.modal('show');
      
       // Center the modal in the middle of the screen.
      this.modal.css({
        'margin-top': this.modal.height() * -0.5,
        'top': '50%'
      });
    },

     createModal: function (text) {
    	 //console.log('point 18')
      //if (!this.modal) {
      // re-create modal everytime it is called
        var element = this.modal = jQuery(this.options.template);
        element.on('click', '.btn-primary', this._onConfirmSuccess);
        element.on('click', '.btn-cancel', this._onConfirmCancel);
        element.modal({show: false});
        element.find('h3').text(this.i18n('heading'));
        element.find('.modal-body').text(text);
        
        element.find('.btn-primary').text(this.i18n('confirm'));
        element.find('.btn-cancel').text(this.i18n('cancel'));
      //}
      return this.modal;
    },

    /* Event handler for the success event */
    _onConfirmSuccess: function (e) {
    	//console.log('point 19')
        var h = this.options.helper;
        var action = this.options.action;
        var options = this.options.options;
        var ld = this.options.ld;
        var cb = this.options.cb;
        this.sandbox.body.append(h[action](options, ld, cb));
        this.modal.modal('hide');
    },

    /* Event handler for the cancel event */
    _onConfirmCancel: function (event) {
    	//console.log('point 20')
      this.modal.modal('hide');
    },
    _onRepaint: function(columns){
    	//console.log('point 21')
        var header = jQuery(".data-view-container .slick-header .slick-column-name");
        var self = this;
        for (var key in columns){
            var mode = columns[key];

        //}
        
        header.each(function(idx){
            var col = jQuery(this);
            //console.log(col.text());
            col.parent().css("background-image", "none"); 
            //col.parent().css("background-color","red");
            if (col.text().startsWith(key)){
                if (mode === 'no-translate'){
                    col.parent().css("background-color","red");
                }
                else if (mode === 'manual'){
                    col.parent().css("background-color","blue");
                }
                else if (mode === 'automatic'){
                    col.parent().css("background-color","yellow");
                }
                else if (mode === 'transcription'){
                    col.parent().css("background-color","green");
                }
                else{
                    col.parent().css("background-color","grey");
                }
            }
        
            });
            }
        }
  };
});




