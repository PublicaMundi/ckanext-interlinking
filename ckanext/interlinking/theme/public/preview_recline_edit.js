// recline preview module
var dataExplorer;

var errorMsg;

var interlinking_utility = interlinking_utility || {};

this.ckan.module('recline_interlink_preview', function (jQuery, _) {  
  return {
    options: {
      i18n: {
        heading_confirm: _("Please Confirm Action"),
        heading_notify: _("Please Note"),
        datastore_disabled: _("Datastore is disabled. Please enable datastore and try again in order to proceed with resource interlinking"),
        confirm_delete: _("Are you sure you want to restore this column to its status before interlinking take place?"),
        confirm_update: _("Are you sure you want to update existing column interlinking?"),
        confirm_update: _("Are you sure you want to restart interlinking on this column?"),
        confirm_finalize: _("Finalizing the interlinking process for a column means that its contents will be update accordingly. Are you sure you want to finalize interlinking for this column?"),
        confirm_applyToAll_partA: _("Are you sure you wish to set the value for field \""),
        confirm_applyToAll_partB: _("\" equal to \""),
        confirm_applyToAll_partC: _("\" for all rows where field \""),
        confirm_applyToAll_partD: _("\" is equal to \""),
        confirm_applyToAll_partE: _("\" ?"),
        errorLoadingPreview: _("Could not load preview"),
        errorDataProxy: _("DataProxy returned an error"),
        errorDataStore: _("DataStore returned an error"),
        interlinkWith: _("Interlink with:"),
        interlinkChoices: _("Choices:"),
        searchAnotherTerm: _("Search for another matching term:"),
        type3characters: _("Type at least 3 characters..."),
        applyAllMatchingTerms: _("Use selected value for all matching cells"),
        notCompleteInterlinkNotePartA: _("Interlinking process for this resource cannot be yet finalized. Field '"),
        notCompleteInterlinkNotePartB: _("' has some remaining blank cells."),
        ok: _("Ok"),
        previewNotAvailableForDataType: _("Preview not available for data type: ")
      },
    template_confirm: [
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

    template_notify: [
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
         '<a href="#" class="btn btn-primary" id="okClicked">OK</a>',
         '</div>',
         '</div>'
       ].join('\n'),

       site_url: ""
     },

    initialize: function () {
      jQuery.proxyAll(this, /_on/);
      this.save_btn = jQuery("#saveClicked");
      this.publish_btn = jQuery("#publishClicked");
      this.finalize_btn = jQuery("#finalizeClicked");
      this.abort_btn = jQuery("#abortClicked");

      
      // Initializing object interlinking_utility
      // i18n strings
      interlinking_utility.i18n = interlinking_utility.i18n || {};
      interlinking_utility.i18n['interlinkWith'] = this.i18n('interlinkWith');
      interlinking_utility.i18n['interlinkChoices'] = this.i18n('interlinkChoices');
      interlinking_utility.i18n['searchAnotherTerm'] = this.i18n('searchAnotherTerm');
      interlinking_utility.i18n['type3characters'] = this.i18n('type3characters');
      interlinking_utility.i18n['applyAllMatchingTerms'] = this.i18n('applyAllMatchingTerms');
      
      // interlinking state
      interlinking_utility.int_state = interlinking_utility.int_state || {};
      // This indicates the current field which is being interlinked. If it is undefined then no 
      //  is being interlinked.
      //interlinking_utility.int_state['interlinked_column'] = undefined;
      
      this.el.ready(this._onReady);
    },
    _onReady: function() {
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
        

        
        if(!(resourceData.on_interlinking_process == 'True')){
            //console.log("Create new");
        	if (typeof interlinking_utility.int_state != 'undefined' && 
        			typeof interlinking_utility.int_state['interlinked_column'] != 'undefined')
        		delete interlinking_utility.int_state['interlinked_column'];
            var resourceIntData = int_helper.create(function() {}, function() {             	
            	self.options.res_interlink_id = resourceData.temp_interlinking_resource = resourceIntData.responseJSON.result.id;
                dataset = new recline.Model.Dataset(resourceData);
            	self.initializeDataset(dataset, resourceData);
            });
        }else{
        	this.options.res_interlink_id = resourceData.temp_interlinking_resource;
            var translationResource = null;
            this.initializeDataset(dataset, resourceData);
        }
      }
      else{
          //TODO: doesn't work for some reason
          self.sandbox.notify(self.i18n('datastore_disabled'), 'error');
      }
    },
    
    _onCompleteShow: function(res){
        var self = this;
        if (this.options.action == 'delete'){
        	window.top.location.href = this.options.options.return_url;
        } else if (this.options.action == 'finalize' ){
        	window.top.location.href = this.options.options.return_url + res.responseJSON.result.interlinked_res_id;
        } else{
	        dataExplorer.model.fetch().done(function(dataset){
	            var res = {id: self.options.res_interlink_id, endpoint: self.options.site_url + 'api'};
	            int_helper.show_resource(res, function(response){
	                if (response){
	                    var columns= {};
	                    try{
	                        columns = JSON.parse(response.responseJSON.result.interlinking_columns_status);
	                        self.options.columns = columns;
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
        var results = results.responseJSON.result
        for (var res in results){
        	ref = results[res]
        }
    },
    _onLoad: function(){
        dataExplorer.notify({message: 'Loading', loader:true, category: 'warning', persist: true});
        setTimeout(function(){ dataExplorer.model.fetch()}, 3000);
    },

    initializeDataset: function(dataset, resourceData) {
        var self = this;
	    function showError(msg){
	        msg = msg || _('error loading preview');
	        window.parent.ckan.pubsub.publish('data-viewer-error', msg);
	    }
	    self.initializeDataExplorer(dataset);
	    
	    dataset.bind('interlink-with', function(col, reference_resource){
            var options = {column_id: col.id, reference_resource: reference_resource};
            self.updateWithConfirmation(dataset, options);
        }); 
	    
	    dataset.bind('finalize-interlinking', function(col){
            var options = {return_url: resourceData.url.substring(0,resourceData.url.indexOf('resource') + 'resource/'.length),
            				resource_id: resourceData.temp_interlinking_resource};
            self.finalizeWithConfirmation(dataset, options);
        });
	    
	    dataset.bind('abort-interlinking', function(col){
        	// The whole interlinking resource must be deleted
	    	var return_url = resourceData.url.substring(0,resourceData.url.indexOf('download'))
	    	if (return_url.length == 0){
	    		return_url = resourceData.url.replace('interlinking/','');
	    	}
        	var options = {
        					return_url: return_url
        					}
            self.deleteWithConfirmation(dataset, options);
        });
	    
	    dataset.bind('applyToAll', function (row_id, originalValue, selectedValue){
	    	var options = {row_id: row_id,
	    					originalValue: originalValue,
	    					selectedValue: selectedValue,
	    					resource_id: resourceData.temp_interlinking_resource
	    				   }
	    	self.applyValueAllMatchingWithConfirmation(dataset, options);
	    });
	    
	    dataset.queryState.bind('save', function(){
            self.sandbox.notify('hello', 'success');
            //self.sandbox.client.favoriteDataset(this.button.val()).done(self._onSuccess);                  
            dataset.save();
        });
	    
	    self.save_btn.click(function() {
            dataset.save();
        });
	    
	    self.finalize_btn.click(function(){
	    	dataset.trigger('finalize-interlinking');
	    });

	    self.abort_btn.click(function(){
	    	dataset.trigger('abort-interlinking');
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
          
    },
    _onSuccess: function(e) {
            },
    _onEditor: function(column) {
                var pos = column.name.indexOf('-il');
        if (pos > -1){
            return  Slick.Editors.Text
        }
        else{
            return null;
        }
    },
    
    initializeDataExplorer: function (dataset) {
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
    	console.log('point 12')	
        var ld = ld || this._onLoad;
        var cb = cb || this._onCompleteShow; 
        this.options.helper = int_helper;
        this.options.action = 'delete';
        this.options.options = options;
        this.options.cb = cb;
        this.options.ld = ld;
        this.confirm(this.i18n('confirm_delete'));
    },
    
    finalizeWithConfirmation: function(dataset, options, ld, cb){
    	console.log('point 13')	
        var ld = ld || this._onLoad;
        var cb = cb || this._onCompleteShow;
        
        this.checkInterlinkingComplete(dataset, options, ld, cb);      
        this.options.helper = int_helper;
        this.options.action = 'finalize';
        this.options.options = options;
        this.options.cb = cb;
        this.options.ld = ld;
        this.confirm(this.i18n('confirm_finalize'));          
    },
    
    applyValueAllMatchingWithConfirmation: function (dataset, options, ld, cp){
    	console.log('Inside applyValueAllMatchingWithConfirmation');
    	console.log(options)
    	var real_options = {
    						resource_id: options.resource_id,
    						row_id: options.row_id
    					}	
    	
    	var ld = ld || this._onLoad;
        var cb = cb || this._onCompleteShow;
    	this.options.helper = int_helper;
        this.options.action = 'applyToAll';
        this.options.options = real_options;
        this.options.cb = cb;
        this.options.ld = ld;
        
        var i18n_message = this.i18n('confirm_applyToAll_partA') + interlinking_utility.int_state['interlinking_temp_column'] +
        					this.i18n('confirm_applyToAll_partB') + options.selectedValue +
        					this.i18n('confirm_applyToAll_partC') + interlinking_utility.int_state['interlinked_column'] +
        					this.i18n('confirm_applyToAll_partD') + options.originalValue +
        					this.i18n('confirm_applyToAll_partE');
        this.confirm(i18n_message);  

    },
    
    publishWithConfirmation: function(ld, cp) {
    	    	var interlinking_references = int_helper.get_interlinking_references(function() {}, this._onCompleteGetInterlinkingReferences)
    	
    },
    
    // TOCHECK: Currently not used. Is it usefull to retain it?
    updateWithConfirmation: function(dataset, options, ld, cb) {
    	    console.log('point 14')
            var ld = ld || this._onLoad;
            var cb = cb || this._onCompleteShow;
            this.options.helper = int_helper;
            this.options.action = 'update';
            this.options.options = options;
            this.options.ld = ld;
            this.options.cb = cb;
            this.confirm(this.i18n('confirm_update'));
    },
    
    checkInterlinkingComplete: function(dataset, options, ld, cb){
    	console.log(options)
    	var records = dataset.records;
        int_helper['check_interlink_complete'](options, function (){}, this._onCompleteCheckInterlinkingComplete(options, ld, cb));
    },
    
    confirm: function (text) {
      this.sandbox.body.append(this.createModal(text));
      this.modal.modal('show');
      
       // Center the modal in the middle of the screen.
      this.modal.css({
        'margin-top': this.modal.height() * -0.5,
        'top': '50%'
      });
    },

     createModal: function (text) {
    	       //if (!this.modal) {
      // re-create modal everytime it is called
        var element = this.modal = jQuery(this.options.template_confirm);
        element.on('click', '.btn-primary', this._onConfirmSuccess);
        element.on('click', '.btn-cancel', this._onConfirmCancel);
        element.modal({show: false});
        element.find('h3').text(this.i18n('heading_confirm'));
        element.find('.modal-body').text(text);
        
        element.find('.btn-primary').text(this.i18n('confirm'));
        element.find('.btn-cancel').text(this.i18n('cancel'));
      //}
      return this.modal;
    },
    
    createNotificationModal: function (text) {
       var element = this.modal = jQuery(this.options.template_notify);
       element.on('click', '.btn-primary', this._onConfirmCancel);
       element.modal({show: false});
       element.find('h3').text(this.i18n('heading_notify'));
       element.find('.modal-body').text(text);
       element.find('.btn-primary').text(this.i18n('ok'));
     return this.modal;
    },

    /* Event handler for the success event */
    _onConfirmSuccess: function (e) {
    	var h = this.options.helper;
        var action = this.options.action;
        var options = this.options.options;
        var ld = this.options.ld;
        var cb = this.options.cb;
        this.modal.modal('hide');
        this.sandbox.body.append(h[action](options, ld, cb));
    },

    /* Event handler for the cancel event */
    _onConfirmCancel: function (event) {
    	      this.modal.modal('hide');
    },
    _onRepaint: function(columns){
    	        var header = jQuery(".data-view-container .slick-header .slick-column-name");
        var self = this;
        for (var key in columns){
            var mode = columns[key];

        //}
        
        header.each(function(idx){
            var col = jQuery(this);
            col.parent().css("background-image", "none"); 
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
        }, 
    _onCompleteCheckInterlinkingComplete (options, ld, cb){
        	var self = this;
        	console.log('Inside _onCompleteCheckInterlinkingComplete')
        	return function (result){
	        	if (result.responseJSON.result === false){
	        		console.log('false!')
	        		var text = self.i18n('notCompleteInterlinkNotePartA') + options.column_name + self.i18n('notCompleteInterlinkNotePartB'); 
	        		self.sandbox.body.append(self.createNotificationModal(text));
	        		self.modal.modal('show');
	        	    // Center the modal in the middle of the screen.
	        		self.modal.css({
	        	        'margin-top': self.modal.height() * -0.5,
	        	        'top': '50%'
	        	      });
	        	}else{
	        		/*
	        		console.log('true!')
        		 	self.options.helper = int_helper;
        			self.options.action = 'finalize';
        			self.options.options = options;
        			self.options.cb = cb;
        			self.options.ld = ld;
        			self.confirm(self.i18n('confirm_finalize'));
        			*/
	        	}
	        }
        }
  };
});




