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

    res = p.toolkit.get_action('resource_show')(context, {'id': data_dict.get('resource_id')})
    ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': data_dict.get('resource_id')})    
    on_interlinking_process = res.get('on_interlinking_process')  
    print 'on_interlinking_process: ', on_interlinking_process
    if on_interlinking_process == True:
        log.info('Resource {0} is already subject to an ongoing interlinking process. In order to start a new,' 
                 'the current one must be completed.'.format(data_dict.get('id')))
        raise p.toolkit.ValidationError('Interlinking resource already exists')

    ### TODO: resource_show doesn't display package_id until CKAN 2.3
    # Now demanding package_id parameter
    # In case resource doesn't provide a name, use id instead

    # Create resource if it doesn't exist with proper metadata
    if res.get('name') is not None:
        new_res_name = res.get('name') + ' (interlinking)'
    else:
        new_res_name = res.get('id') + ' (interlinking)'

    fields = ds.get('fields')
    columns_status = {}
    for field in fields:
        col = {field.get('id')}
        if field.get('type') != 'text':
            columns_status.update({field.get('id'):'not-interlinkable'})
        else:
            columns_status.update({field.get('id'):'not-interlinked'})

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
                'state': 'active',  #TOCHECK: Is it used?            
                'interlinking_columns_status':columns_status,
                'interlinking_columns': '{}' #TOCHECK: Is it needed?
            })
    
    temp_interlinking_resource = new_res.get('id')    
    
    # Update original resource metadata
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

    #records = data_dict.pop('records', None)
    data_dict, errors = _validate(data_dict, schema, context)
    #if records:
    #    data_dict['records'] = records
    if errors:
        raise p.toolkit.ValidationError(errors)

    res = p.toolkit.get_action('resource_show')(context, {'id': data_dict.get('resource_id')})
    
    # Check if candidate resource is interlinking resource
    if not res.get('interlinking_resource'):
        raise p.toolkit.ValidationError('Resource "{0}" is not an interlinking resource'.format(res.get('id')))
    
    original_res = p.toolkit.get_action('resource_show')(context, {'id': res.get('interlinking_parent_id')})

    ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': data_dict.get('resource_id')})
    original_ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': res.get('interlinking_parent_id')})

    # Check if column_name exists in original table
    col_name = data_dict.get('column_name')
    reference_resource = data_dict.get('reference_resource')
    field_exists = False
    for field in original_ds.get('fields'):
        if field['id'] == col_name:
            field_exists = True
            break
    if not field_exists:
        raise p.toolkit.ValidationError('Column name "{0}" does not correspond to any "{1}" table columns'.format(data_dict.get('column_name'),res.get('interlinking_parent_id')))
    
    
    #Check if the column is not-interlinkable
    columns = json.loads(res.get('interlinking_columns_status','{}'))
    pprint.pprint(columns)
    if columns[col_name] == 'not-interlinkable':
        raise p.toolkit.ValidationError('Column name "{0}" cannot be interlinked'.format(col_name))
       

    _initialize_column(context, col_name, ds, original_ds.get('total'))
    _interlink_column(context, res, col_name, original_ds, ds, reference_resource)
    return


def interlinking_resource_delete(context, data_dict):
    '''Delete a column or the whole resource given an (interlinking) resource_id and/or column_name

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
        ds = p.toolkit.get_action('datastore_search')(context, {'id':data_dict.get('resource_id')})

        # Check if column_name exists in original table
        col_name = data_dict.get('column_name')
        
        #Checking if the column is not-interlinkable
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
                columns.update({k:'not-interlinked'})
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

        filters = {col_name:'*'}
        return p.toolkit.get_action('datastore_delete')(context, {'resource_id': data_dict.get('resource_id'), 'filters':filters, 'force':True})

    # Delete datastore table
    try:        
        p.toolkit.get_action('datastore_delete')(context, {'resource_id': data_dict.get('resource_id'), 'filters':filters, 'force':True})
    except:
        return
    # Update metadata and delete resource
    temp_interlinking_resource = original_res.get('temp_interlinking_resource')
    if not temp_interlinking_resource:
        raise p.toolkit.ValidationError('Original resource has no interlinking metadata. Something went wrong...')

    upd_original_res = p.toolkit.get_action('resource_update')(context, {
        'id':original_res.get('id'),
        'url_type': original_res.get('url_type'),
        'on_interlinking_process': False,
        'url':original_res.get('url'),
        'format':original_res.get('format'),
        })
    return p.toolkit.get_action('resource_delete')(context, {'id': data_dict.get('resource_id')})


def interlinking_resource_finalize(context, data_dict):
    '''Finalizes the interlinked resource, i.e. every (original) column under interlinking, it is replaced 
        by the interlinked one.
    
    '''
    p.toolkit.check_access('interlinking_resource_finalize', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_resource_finalize_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)

    #package = p.toolkit.get_action('dataset_show')(context, {'id': data_dict.get('package_id')})
    original_resource = data_dict.get('resource_id')
    res = p.toolkit.get_action('resource_show')(context, {'id': original_resource})

    on_interlinking_process = res.get('on_interlinking_process')
    if not on_interlinking_process or on_interlinking_process == False:
        raise p.toolkit.ValidationError('Resource "{0}" is not currently being interlinked resource'.format(res.get('id')))
    
    # Copying records from the temporary interlinking resource to the original one
    pprint.pprint(res)
    temp_interlinking_resource = res.get('temp_interlinking_resource')
    print original_resource, temp_interlinking_resource
       
    """
    print '>>>>>>>>>>>>>>>>>>>>>>>>>>>ORIGINAL>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
    pprint.pprint(original_ds)
    print '>>>>>>>>>>>>>>>>>>>>>>>>>>>INTERLINKED>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'
    pprint.pprint(temp_int_ds)
    """
    STEP = 100
    offset = 0
    original_ds = p.toolkit.get_action('datastore_search')(context, {'resource_id':original_resource, 'offset': offset, 'sort':'_id'})
    temp_int_ds = p.toolkit.get_action('datastore_search')(context, {'resource_id':temp_interlinking_resource, 'offset': offset, 'sort':'_id'})
    total = original_ds.get('total')
    
    for k in range(1, total/STEP+2):
        recs = p.toolkit.get_action('datastore_search')(context, {'resource_id':original_resource, 'offset': offset, 'sort':'_id'}).get('records')
        for rec in recs:
            filter = {'_id':rec.get('_id')}
            int_rec = p.toolkit.get_action('datastore_search')(context, {'resource_id':temp_interlinking_resource, 'filter':filter})
            pprint.pprint(filter)
            pprint.pprint(int_rec)
                
    
    """ 
    
    for k in range(1,total/STEP+2):
        recs = p.toolkit.get_action('datastore_search')(context, {'resource_id':res_id, 'offset': offset, 'sort':'_id'}).get('records')
        nrecs = []
        for rec in recs:
            original_term = rec.get(col_name)
            suggestions = solr_access.spell_search(original_term, reference)
            #pprint.pprint(suggestions)
            #for s in suggestions:
            #    print s.encode('utf8')
            suggestions_str = ','.join(suggestions)
            nrec = {'_id':rec.get('_id'),col_name:suggestions_str}
            nrecs.append(nrec)
             
        ds = p.toolkit.get_action('datastore_upsert')(context,
                {
                    'resource_id': new_ds.get('resource_id'),
                    'allow_update_with_id':True,
                    'force': True,
                    'records': nrecs
                    })
                
        offset=offset+STEP
    """

    # Update resource metadata
    """
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
    """
    return


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


def _interlink_column(context, res, col_name, original_ds, new_ds, reference):
    pprint.pprint(res)
    res_id = original_ds.get('resource_id')
    print 'res_id', res_id
    total = original_ds.get('total')
    print 'total: ', total
    print 'reference: ', reference
    columns = json.loads(res.get('interlinking_columns_status','{}'))
    pprint.pprint(columns)
    
    # The interlinked column is marked with the reference resource with which it is interlinked.
    for k,v in columns.iteritems():
        if k == col_name:
            columns.update({k:reference})
    columns = json.dumps(columns)
    pprint.pprint(columns)
    res = p.toolkit.get_action('resource_update')(context, {
            'id': res.get('id'),
            'url': res.get('url'),
            'format': res.get('format'),
            'interlinking_parent_id': res.get('interlinking_parent_id'),
            'interlinking_resource': True,
            'interlinking_status': res.get('interlinking_status'),
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
            suggestions = solr_access.spell_search(original_term, reference)
            #pprint.pprint(suggestions)
            #for s in suggestions:
            #    print s.encode('utf8')
            suggestions_str = ','.join(suggestions)
            nrec = {'_id':rec.get('_id'),col_name:suggestions_str}
            nrecs.append(nrec)
             
        ds = p.toolkit.get_action('datastore_upsert')(context,
                {
                    'resource_id': new_ds.get('resource_id'),
                    'allow_update_with_id':True,
                    'force': True,
                    'records': nrecs
                    })
                
        offset=offset+STEP
    return new_ds
    
        

