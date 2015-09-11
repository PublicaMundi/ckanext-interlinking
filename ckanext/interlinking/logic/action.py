#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import shlex

import pylons
import sqlalchemy
import json
import pprint

from unidecode import unidecode
import ckan.lib.navl.dictization_functions
import ckan.logic as logic
import ckan.plugins as p
import ckanext.interlinking.logic.schema as dsschema
import ckanext.interlinking.logic.solr_access as solr_access

import uuid
#from ckan.lib.celery_app import celery
if not os.environ.get('DATASTORE_LOAD'):
    import paste.deploy.converters as converters
    ValidationError = p.toolkit.ValidationError
else:
    log.warn("Running datastore without CKAN")

    class ValidationError(Exception):
        def __init__(self, error_dict):
            pprint.pprint(error_dict)

log = logging.getLogger(__name__)
_get_or_bust = logic.get_or_bust
_validate = ckan.lib.navl.dictization_functions.validate


def interlinking_resource_create(context, data_dict):
    '''Creates a new resource and creates a datastore table associated to
    the original resource whose id is provided

    '''

    p.toolkit.check_access('interlinking_resource_create', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_resource_create_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)

    #package = p.toolkit.get_action('dataset_show')(context, {'id': data_dict.get('package_id')})
    res = p.toolkit.get_action('resource_show')(context, {'id': data_dict.get('resource_id')})
    #try:
    # Check if datastore table exists
    ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': data_dict.get('resource_id')})
    
    
    #except p.toolkit.ObjectNotFound:
    #    log.info('Resource {0} does not have a datastore table associated with it'.format(data_dict.get('id')))
    #    return
    on_interlinking_process = res.get('on_interlinking_process')  

    
    if on_interlinking_process:
        log.info('Resource {0} is already subject to an ongoing interlinking process. In order to start a new,' 
                 'the current one must be completed.'.format(data_dict.get('id')))
        raise p.toolkit.ValidationError('Interlinking resource already exists')

    ### TODO: resource_show doesnt display package_id until CKAN 2.3
    # Now demanding package_id parameter
    # In case resource doesnt provide a name, use id instead

    # Create resource if it doesnt exist with proper metadata
    if res.get('name') is not None:
        new_res_name = res.get('name') + ' (interlinking)'
    else:
        new_res_name = res.get('id') + ' (interlinking)'

    fields = ds.get('fields')
    columns_status = {}
    for field in fields:
        col = {field.get('id')}
        if field.get('type') != 'text':
            columns_status.update({field.get('id'):'non-interlinked'})
        else:
            columns_status.update({field.get('id'):''})

    columns_status = json.dumps(columns_status)
    
    new_res = p.toolkit.get_action('resource_create')(context,
            {
                'package_id': data_dict.get('package_id'),
                'url':'http://',
                'format':'data_table',
                'name': new_res_name,
                'description': 'This is a resource created for interlinking purposes',
                'interlinking_parent_id': data_dict.get('resource_id'),
                'interlinking_resource': True,
                'interlinking_status': 'draft', #TOCHECK: Is it needed?
                'state': 'active',              
                'interlinking_columns_status':columns_status,
                'interlinking_columns': '{}',
            })
       
     
    # Update original resource metadata
    temp_interlinking_resource = new_res.get('id')
    res = p.toolkit.get_action('resource_update')(context,
            {
                'id':res.get('id'),
                'temp_interlinking_resource': temp_interlinking_resource,
                'on_interlinking_process': True,
                'format': res.get('format'),
                'url': res.get('url')
                }
            )
     

    # Initialize empty datastore table associated to resource
    new_ds = p.toolkit.get_action('datastore_create')(context,
            {
                'resource_id': new_res.get('id'),
                'force':True,
            })
    
    return new_res
    


def interlinking_resource_update(context, data_dict):
    '''Update or insert column given the resource_id, column_name
    and interlinking reference resource

    '''
    p.toolkit.check_access('interlinking_resource_update', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_resource_update_schema())
    print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>data_dict<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'
    print data_dict
    #records = data_dict.pop('records', None)
    data_dict, errors = _validate(data_dict, schema, context)
    #if records:
    #    data_dict['records'] = records
    if errors:
        raise p.toolkit.ValidationError(errors)

    res = p.toolkit.get_action('resource_show')(context, {'id': data_dict.get('resource_id')})
    print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>res<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'
    print res
    
    # Check if candidate resource is translation resource
    if not res.get('interlinking_resource'):
        raise p.toolkit.ValidationError('Resource "{0}" is not an interlinking resource'.format(res.get('id')))
    
    original_res = p.toolkit.get_action('resource_show')(context, {'id': res.get('interlinking_parent_id')})

    ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': data_dict.get('resource_id')})
    original_ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': res.get('interlinking_parent_id')})

    print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>original_ds<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'
    pprint.pprint( original_ds)
    print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ds<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'
    pprint.pprint( ds)
    # Check if column_name exists in original table
    col_name = data_dict.get('column_name')
    field_exists = False
    for field in original_ds.get('fields'):
        if field['id'] == col_name:
            field_exists = True
            break
    if not field_exists:
        raise p.toolkit.ValidationError('Column name "{0}" does not correspond to any "{1}" table columns'.format(data_dict.get('column_name'),res.get('interlinking_parent_id')))

    

    ## 
    ##  Interlinking logic goes here
    ##

    #_initialize_column(context, col_name, ds, original_ds.get('total'))
    #return solr.
    
    _interlink_column(context, res, col_name, original_ds, ds, 'cities2')
    
    #if mode == 'manual':
    #    return _translate_manual(context, res, col_name, original_ds, ds)
    #elif mode == 'automatic':
    #    return _translate_automatic(context, res, ds)
    #elif mode == 'transcription':
    #    _transcript(context, res, col_name, original_ds, ds)
    #else:
    #    log.info('Should never reach here')
    #    return
    
    return

def interlinking_resource_delete(context, data_dict):
    '''Delete a column or the whole resource given a (translation) resource_id and/or column_name

    '''
    p.toolkit.check_access('interlinking_resource_delete', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_resource_delete_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)

    res = p.toolkit.get_action('resource_show')(context, {'id': data_dict.get('resource_id')})
    if not res.get('interlinking_resource'):
        raise p.toolkit.ValidationError('Resource "{0}" is not an interlinking resource'.format(res.get('id')))

    original_res = p.toolkit.get_action('resource_show')(context, {'id': res.get('interlinking_parent_id')})

    # Delete column if option is set
    # TODO: datastore doesnt support deleting a whole column - dont support this
    filters = {}
    if 'column_name' in data_dict:
        #filters = {data_dict.get('column_name'):'*'}
        # Delete datastore table
        print 'Delete only column!'

        ds = p.toolkit.get_action('datastore_search')(context, {'id':data_dict.get('resource_id')})
        #total = ds.get('total')
        # Check if column_name exists in original table
        col_name = data_dict.get('column_name')
        field_exists = False
        for field in ds.get('fields'):
            if field['id'] == col_name:
                field_exists = True
                break
        if not field_exists:
            raise p.toolkit.ValidationError('Column name "{0}" does not correspond to any "{1}" table columns'.format(col_name, ds.get('resource_id')))

        columns = json.loads(res.get('interlinking_columns_status','{}'))
        for k,v in columns.iteritems():
            if k == col_name:
                columns.update({k:'no-interlinking'})

        columns = json.dumps(columns)
        res = p.toolkit.get_action('resource_update')(context, {
                'id': res.get('id'),
                'url': res.get('url'),
                'format': res.get('format'),
                'interlinking_parent_id': res.get('interlinking_parent_id'),
                'interlinking_resource': True,
                'interlinking_language': res.get('interlinking_language'),
                'interlinking_status': res.get('interlinking_status'),
                'interlinking_columns_status':columns,
                'interlinking_columns':res.get('interlinking_columns'),
                })

        print 'ASDASADSD'
        filters = {col_name:'*'}
        #la = _delete_column(context, data_dict.get('column_name'), ds, total)
        #return
        print 'after initialize'
        print col_name
        #pprint.pprint(la)
        #filters = {}
        return p.toolkit.get_action('datastore_delete')(context, {'resource_id': data_dict.get('resource_id'), 'filters':filters, 'force':True})

    # Delete datastore table
    try:
        p.toolkit.get_action('datastore_delete')(context, {'resource_id': data_dict.get('resource_id'), 'filters':filters, 'force':True})
    except:
        return

    # Update metadata and delete resouce
    being_interlinked_with = original_res.get('being_interlinked_with')
    if not being_interlinked_with:
        raise p.toolkit.ValidationError('Original resource has no translation metadata. Something went wrong...')

    upd_original_res = p.toolkit.get_action('resource_update')(context, {
        'id':original_res.get('id'),
        'url_type': original_res.get('url_type'),
        'url':original_res.get('url'),
        'format':original_res.get('format'),
        })
    return p.toolkit.get_action('resource_delete')(context, {'id': data_dict.get('resource_id')})

def interlinking_resource_publish(context, data_dict):
    '''Publishes the translation resource
    by changing its state
    '''
    p.toolkit.check_access('interlinking_resource_publish', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_resource_publish_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)

    #package = p.toolkit.get_action('dataset_show')(context, {'id': data_dict.get('package_id')})
    res = p.toolkit.get_action('resource_show')(context, {'id': data_dict.get('resource_id')})

    if not res.get('interlinking_resource'):
        raise p.toolkit.ValidationError('Resource "{0}" is not an interlinking resource'.format(res.get('id')))

    # Update resource metadata
    p.toolkit.get_action('resource_update')(context, {
        'id':res.get('id'),
        'url':res.get('url'),
        'format':res.get('format'),
        'interlinking_parent_id': res.get('interlinking_parent_id'),
        'interlinking_resource': True,
        'interlinking_language': res.get('interlinking_language'),
        'interlinking_status': 'published',
        'interlinking_columns': res.get('interlinking_columns'),
        })
    return

def interlinking_resource_unpublish(context, data_dict):
    '''Unpublishes the translation resource
    by changing its state
    '''
    p.toolkit.check_access('interlinking_resource_publish', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_resource_publish_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)

    #package = p.toolkit.get_action('dataset_show')(context, {'id': data_dict.get('package_id')})
    res = p.toolkit.get_action('resource_show')(context, {'id': data_dict.get('resource_id')})

    if not res.get('interlinking_resource'):
        raise p.toolkit.ValidationError('Resource "{0}" is not an interlinking resource'.format(res.get('id')))

    # Update resource metadata
    p.toolkit.get_action('resource_update')(context, {
        'id':res.get('id'),
        'url':res.get('url'),
        'format':res.get('format'),
        'interlinking_parent_id': res.get('interlinking_parent_id'),
        'interlinking_resource': True,
        'interlinking_language': res.get('interlinking_language'),
        'interlinking_status': 'draft',
        'interlinking_columns': res.get('interlinking_columns'),
        })
    return

def _interlink_column(context, res, col_name, original_ds, new_ds, reference):
    print '________________________________________________________________'
    print col_name
    res_id = original_ds.get('resource_id')
    total = original_ds.get('total')
    columns = json.loads(res.get('interlinked_with','{}'))
    for k,v in columns.iteritems():
        if k == col_name:
            columns.update({k:reference})
    columns = json.dumps(columns)
    res = p.toolkit.get_action('resource_update')(context, {
            'id': res.get('id'),
            'url': res.get('url'),
            'format': res.get('format'),
            'interlinking_parent_id': res.get('interlinking_parent_id'),
            'interlinking_resource': True,
            'translation_status': res.get('translation_status'),
            'interlinking_columns_status':columns,
            'interlinking_columns':res.get('interlinking_columns'),
            })
    
    STEP = 100
    offset = 0
    
    
    for k in range(1,total/STEP+2):
        recs = p.toolkit.get_action('datastore_search')(context, {'resource_id':res_id, 'offset': offset, 'sort':'_id'}).get('records')
        nrecs = []
        for rec in recs:
            original_term = rec.get(col_name)
            print '-----------------------------------------------------------------'
            print original_term.encode('utf8')
            suggestions = solr_access.spell_search(original_term, reference)
            for s in suggestions:
                print s.encode('utf8')

    """
    term = u'Μέγαρ'
    suggestions = solr_access.spell_search(term,  reference) 
    for s in suggestions:
        print s.encode('utf8')
    """
    
    

def _transcript(context, res, col_name, original_ds, new_ds):
    # TODO: Need to json serialize context and data_dict
    print 'HELLO Transliterate'
    res_id = original_ds.get('resource_id')
    total = original_ds.get('total')
    columns = json.loads(res.get('translation_columns_status','{}'))
    for k,v in columns.iteritems():
        if k == col_name:
            columns.update({k:'transcription'})
    columns = json.dumps(columns)
    res = p.toolkit.get_action('resource_update')(context, {
            'id': res.get('id'),
            'url': res.get('url'),
            'format': res.get('format'),
            'translation_parent_id': res.get('translation_parent_id'),
            'translation_resource': True,
            'translation_language': res.get('translation_language'),
            'translation_status': res.get('translation_status'),
            'translation_columns_status':columns,
            'translation_columns':res.get('translation_columns'),
            })
    STEP = 100
    offset = 0
    print total/STEP+1
    for k in range(1,total/STEP+2):
        print 'offset'
        print k
        recs = p.toolkit.get_action('datastore_search')(context, {'resource_id':res_id, 'offset': offset, 'sort':'_id'}).get('records')
        #recs = original_ds.get('records')
        #print original_ds
        #print recs
        #recs = ds.get('records')
        nrecs = []
        for rec in recs:
            key = col_name
            value = rec.get(key)
            nvalue = unidecode(value)
            rec.update({key:nvalue})
            #print 'KEY:VALUE'
            #print key+':'+nvalue
            nrec = {'_id':rec.get('_id'),key:nvalue}
            #print nrec
            nrecs.append(nrec)
            #name = data_dict.get('column_name')

        ds = p.toolkit.get_action('datastore_upsert')(context,
                {
                    'resource_id': new_ds.get('resource_id'),
                    'allow_update_with_id':True,
                    'force': True,
                    'records': nrecs
                    })
        offset=offset+STEP
    return new_ds

def _delete_column(context, col_name, ds, total):
    # And update with correct number of records
    return p.toolkit.get_action('datastore_upsert')(context,
            {
                'resource_id': ds.get('resource_id'),
                'force':True,
                'method':'upsert',
                'allow_update_with_id':True,
                'records': [{'_id':i, col_name:None} for i in range(1,total+1)]
            })

def _initialize_column(context, col_name, ds, total):
    
    fields = ds.get('fields')
    # Remove _id from fields list
    fields.pop(0)
    for field in fields:
        if col_name == field.get('id'):
            return
    
    # Build fields list
    new_column = {'id': col_name,
                'type': 'text'}
    fields.append(new_column)
    
    # Update fields with datastore_create
    
    new_ds = p.toolkit.get_action('datastore_create')(context,
            {
                'resource_id': ds.get('resource_id'),
                'force':True,
                'allow_update_with_id':True,
                'fields': fields
                #'records':[{col_name:''}]
                })
    
    return
    # And update with correct number of records
    #return p.toolkit.get_action('datastore_upsert')(context,
    #        {
    #            'resource_id': ds.get('resource_id'),
    #            'force':True,
    #            'method':'upsert',
    #            'allow_update_with_id':True,
    #            'records': [{'_id':i, col_name:''} for i in range(1,total+1)]
    #        })


#def datastore_make_private(context, data_dict):
#def datastore_make_public(context, data_dict):

def _resource_exists(context, data_dict):
    ''' Returns true if the resource exists in CKAN and in the datastore '''
    model = _get_or_bust(context, 'model')
    res_id = _get_or_bust(data_dict, 'resource_id')
    if not model.Resource.get(res_id):
        return False

    resources_sql = sqlalchemy.text(u'''SELECT 1 FROM "_table_metadata"
                                        WHERE name = :id AND alias_of IS NULL''')
    results = db._get_engine(data_dict).execute(resources_sql, id=res_id)
    return results.rowcount > 0

def _get_field_ids(field_arr):
    l = []
    for it in field_arr:
        #if not it['id'] == '_id':
        l.append(it['id'])
    return l

def _check_read_only(context, data_dict):
    ''' Raises exception if the resource is read-only.
    Make sure the resource id is in resource_id
    '''
    if data_dict.get('force'):
        return
    res = p.toolkit.get_action('resource_show')(
        context, {'id': data_dict['resource_id']})
    if res.get('url_type') != 'datastore':
        raise p.toolkit.ValidationError({
            'read-only': ['Cannot edit read-only resource. Either pass'
                          '"force=True" or change url-type to "datastore"']
        })
