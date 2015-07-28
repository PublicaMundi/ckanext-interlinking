// recline preview module
var dataExplorer;
//var LANGUAGE =  'fr';
var errorMsg;

this.ckan.module('recline_interlink_preview', function (jQuery, _) {  
  return {
    options: {
      i18n: {
        heading: _("Please Confirm Action"),
        datastore_disabled: _("Datastore is disabled. Please enable datastore and try again in order to proceed with resourcer interlinkingm"),
        confirm_delete: _("Are you sure you want to delete column interlinking?"),
        confirm_update: _("Are you sure you want to update existing column interlinking?"),
        confirm_publish: _("Are you sure you want to publish resource interlinking?"),
        errorLoadingPreview: _("Could not load preview"),
        errorDataProxy: _("DataProxy returned an error"),
        errorDataStore: _("DataStore returned an error"),
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
      jQuery.proxyAll(this, /_on/);
      
      this.save_btn = jQuery("#saveClicked");
      this.publish_btn = jQuery("#publishClicked");
      this.el.ready(this._onReady);
    },
    _onReady: function() {
      this.loadPreviewDialog(preload_resource);  
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
        if (!("interlinking_resource" in resourceData)){
            resourceData.backend =  'ckanInterlinkEdit';
        }
        else{
            resourceData.backend =  'ckan';
        }
        // Set endpoint of the resource to the datastore api (so it can locate
        // CKAN DataStore)
        //resourceData.endpoint = jQuery('body').data('site-root') + 'api';
        resourceData.endpoint = this.options.site_url + 'api';
         
        dataset = new recline.Model.Dataset(resourceData);
         
        errorMsg = this.options.i18n.errorLoadingPreview + ': ' + this.options.i18n.errorDataStore;
        
        translate = new TranslateHelper(resourceData); 
        
        this.options.res_trans_id = res_trans_id = resourceData.being_interlinked_with;

        if (this.options.res_trans_id === undefined  && !("interlinking_resource" in resourceData)){
            console.log("Create new");
            resourceData = translate.create(function() {}, function() { 
            self.initializeDataset(dataset, resourceData);
                //window.location.reload() ;
            });
            //Need reload in order to move on
            //Fix this
        }
        else{
            var translationResource = null;
            this.initializeDataset(dataset, resourceData);
        }
      }
      else{
          //TODO: doesnt work for some reason
          self.sandbox.notify(self.i18n('datastore_disabled'), 'error');
      }
    },
    _onComplete: function(){
            
        var self = this;
        dataExplorer.model.fetch().done(function(dataset){
            //var columns = dataset.fields.models;
            var res = {id: self.options.res_trans_id, endpoint: self.options.site_url + 'api'};
            console.log('asda');
            console.log(res);
            translate.show_resource(res, function(response){ 
                if (response){
                    var columns= {};
                    try{
                        console.log(response.responseJSON.result);
                        columns = JSON.parse(response.responseJSON.result.interlinking_columns_status);
                        self.options.columns = columns;
                        console.log(columns);
                        //self._onRepaint(columns);
                    }
                    catch(err) {
                        alert('oops');
                    }
                }
                else{
                    alert('resource fetch failed');
                }

            });
        });
    },
    _onLoad: function(){
        dataExplorer.notify({message: 'Loading', loader:true, category: 'warning', persist: true});
        //setTimeout(function(){ dataExplorer.model.fetch()}, 3000);
    },

    initializeDataset: function(dataset, resourceData) {
        var self = this;
        
        
     console.log('!!!! 4');
     function showError(msg){
        msg = msg || _('error loading preview');
        window.parent.ckan.pubsub.publish('data-viewer-error', msg);
      }
        dataset.fetch()
              .done(function(dataset1){
                
                var fields1 = dataset1.fields.models;
                var records1 = dataset1.records.models;
                
                self.initializeDataExplorer(dataset1);
                
                dataset.bind('translate-no', function(col){
                    var options = {column:col.name};
                    console.log(options);
                    self.deleteWithConfirmation(dataset1, options); 

                });

                            
                dataset.bind('title', function(col){
                        var col_translation = '';
                        //translate.update({column:col.name, mode:'title', title:col_translation}, self._onLoad, self._onComplete);
                        self.confirm(self.i18n('confirm_update'));
                    
                });
                
                dataset.bind('translate-manual', function(col){
                    var options = {column:col.name, mode:'manual'};
                    console.log(options);
                    self.updateWithConfirmation(dataset1, options); 
                });

                dataset.bind('transcript', function(col){
                    var options = {column:col.name, mode:'transcription'};
                    self.updateWithConfirmation(dataset1, options); 
                });

                dataset.bind('translate-auto', function(col){
                    //TODO
                    var options = {column:col.name, mode:'automatic'};
                    self.updateWithConfirmation(dataset1, options); 
                });
    
                dataset1.queryState.bind('save', function(){
                    //console.log('dataset being saved...');
                    self.sandbox.notify('hello', 'success');
                    //self.sandbox.client.favoriteDataset(this.button.val()).done(self._onSuccess);
                    
                    dataset1.save();
                    //.done(function(){
                        //self._onRepaint(self.options.columns);
                    //});
                });

                self.save_btn.click(function() {
                    //console.log('dataset being saved...');
                    dataset1.save();
                    //self.sandbox.notify('hello', 'success');
                });

                self.publish_btn.click(function() {
                    //TODO: Save before publishing - something doesnt work
                    //dataset.save().done(function(dataset){
                    //translate.publish(self._onLoad, function() { window.top.location.href = resourceData.url.substring(0,resourceData.url.indexOf('resource'))})
                    self.publishWithConfirmation(self._onLoad, function() { window.top.location.href = resourceData.url.substring(0,resourceData.url.indexOf('resource'))}); 
                    //});

                })
        
          })
          .fail(function(error){
            if (error.message) errorMsg += ' (' + error.message + ')';
            showError(errorMsg);
          });
    },
    _onSuccess: function(e) {
        console.log('ae');
        console.log(e);
        console.log(this);
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
      var out = format.toLowerCase();
      out = out.split('/');
      out = out[out.length-1];
      return out;
    },
    _normalizeUrl: function (url) {
      if (url.indexOf('https') === 0) {
        return 'http' + url.slice(5);
      } else {
        return url;
      }
    },
    deleteWithConfirmation: function(dataset, options, ld, cb) {
        //var res = {id:this.options.res_trans_id, endpoint:this.options.resourceData.endpoint};
            var ld = ld || this._onLoad;
            var cb = cb || this._onComplete;
            var field_exists = this.checkFieldExists(dataset, options);

            if (field_exists){
                var col_translation = '';
                this.options.helper = translate;
                this.options.action = 'delete';
                this.options.options = options;
                this.options.cb = cb;
                this.options.ld = ld;
                
                this.confirm(this.i18n('confirm_delete'));

            }
            else{
            }        
    },
    publishWithConfirmation: function(ld, cb) {
        var ld = ld || this._onLoad;
        var cb = cb || this._onComplete;
        this.options.helper = translate;
        this.options.action = 'publish';
        this.options.options = {};
        this.options.cb = cb;
        this.options.ld = ld;
        this.confirm(this.i18n('confirm_publish'));
    },
    updateWithConfirmation: function(dataset, options, ld, cb) {
        //var res = {id:this.options.res_trans_id, endpoint:this.options.resourceData.endpoint};
            var ld = ld || this._onLoad;
            var cb = cb || this._onComplete;
            var field_exists = this.checkFieldExists(dataset, options);

            if (field_exists){
                var col_translation = '';
                this.options.helper = translate;
                this.options.action = 'update';
                this.options.options = options;
                this.options.ld = ld;
                this.options.cb = cb;

                this.confirm(this.i18n('confirm_update'));

            }
            else{
                translate.update(options, this._onLoad, this._onComplete);
            }        
    },
    checkFieldExists: function(dataset, options){
        var col = options.column+'-il';
        var fields = dataset.fields.models;
        var field_exists = false; 
        fields.forEach(function(fld, idx){
            if (fld.id == col){
                field_exists = true;
                return;
            }
        });
        return field_exists;
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
        var h = this.options.helper;
        var action = this.options.action;
        var options = this.options.options;
        var ld = this.options.ld;
        var cb = this.options.cb;
        console.log('callback');
        console.log(cb);   
        this.sandbox.body.append(h[action](options, ld, cb));
      this.modal.modal('hide');
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
            //console.log('key');
            //console.log(key);
        //    console.log(col);
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




