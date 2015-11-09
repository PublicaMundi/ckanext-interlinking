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
from random import uniform
from math import ceil
import ckan.lib.navl.dictization_functions
import ckan.logic as logic
import ckan.plugins.toolkit as toolkit
import ckan.plugins as p
import ckanext.interlinking.logic.schema as dsschema
import ckanext.interlinking.logic.solr_access as solr_access
import ckanext.interlinking.logic.lucene_access as lucene_access

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
    if on_interlinking_process == 'True':
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
                # "interlinking_status" possible states:
                #  "not-started": The interlinking resource has been created but a column has not been chosen for interlinking yet
                #  "undergoing": A column has been chosen for interlinking and the process is undergoing
                'interlinking_status': 'not-started',
                'state': 'active',  
                'interlinking_columns_status':columns_status,
                'interlinking_columns': '{}' #TOCHECK: Is it needed?
            })
    
    temp_interlinking_resource = new_res.get('id')    
    
    # Update original resource metadata
    res = p.toolkit.get_action('resource_show')(context, res)
    res['temp_interlinking_resource'] = temp_interlinking_resource
    res['on_interlinking_process'] = True
    res = p.toolkit.get_action('resource_update')(context, res)
    
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
    
    # Check if candidate resource's interlinking is already undergoing
    #if not res.get('interlinking_status') == 'not-started':    
    #    raise p.toolkit.ValidationError('Resource "{0}" is already being interlinked'.format(res.get('id')))
    
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
    
    ref_fields = _initialize_columns(context, col_name, ds, original_ds.get('total'), reference_resource)
    _interlink_column(context, res, col_name, original_ds, ds, reference_resource, ref_fields)
    return

    
    
def interlinking_resource_delete(context, data_dict):
    '''Delete a whole interlinking resource given its resource_id

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
    try:       
        p.toolkit.get_action('datastore_delete')(context, {'resource_id': data_dict.get('resource_id'), 'filters':filters, 'force':True})
    except:
        return
    # Update metadata and delete resource
    temp_interlinking_resource = original_res.get('temp_interlinking_resource')
    if not temp_interlinking_resource:
        raise p.toolkit.ValidationError('Original resource has no interlinking metadata. Something went wrong...')

    upd_original_res = p.toolkit.get_action('resource_show')(context, original_res)
    upd_original_res['on_interlinking_process'] = False
    del upd_original_res['temp_interlinking_resource']
    del upd_original_res['interlinked_column']
    upd_original_res = p.toolkit.get_action('resource_update')(context, upd_original_res)
    
    return p.toolkit.get_action('resource_delete')(context, {'id': data_dict.get('resource_id')})


def interlinking_resource_finalize(context, data_dict):
    '''Finalizes the interlinked resource, i.e. a new one is created where the original interlinked column 
    is being replaced by the new interlinked one.
    
    '''
    p.toolkit.check_access('interlinking_resource_finalize', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_resource_finalize_schema())
    int_resdata_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)

    interlinked_resource_id = data_dict.get('resource_id')
    int_res = p.toolkit.get_action('resource_show')(context, {'id': interlinked_resource_id})
    interlinking_column = json.loads(int_res.get('reference_fields'))[0]['id']
    original_resource_id = int_res.get('interlinking_parent_id')
    
    res = p.toolkit.get_action('resource_show')(context, {'id': original_resource_id})
    original_url = res.get('url')
    #get original filename
    original_url_splitted = original_url.split('/')
    original_file_name = original_url_splitted[ len(original_url_splitted)-1 ]
    
    on_interlinking_process = res.get('on_interlinking_process')
    #if not on_interlinking_process or on_interlinking_process == False:
    #    raise p.toolkit.ValidationError('Resource "{0}" is not currently being interlinked resource'.format(res.get('id')))
    
    #TODO get a better name for reference
    interlinking_columns_status = json.loads(int_res.get('interlinking_columns_status'))
    for i in interlinking_columns_status:
        if interlinking_columns_status[i] != 'not-interlinked':
            interlinked_column = i
            interlinking_reference  = interlinking_columns_status[i]
            
    #if not interlinked_column:
    #    raise p.toolkit.ValidationError('Resource "{0}" does not have an interlinked column'.format(res.get('id')))
    
    original_name = res.get('name')
    
    if res.get('interlinked_resource'):
        interlinking_lineage = json.loads(res.get('interlinking_lineage'))
    else:
        interlinking_lineage = []  
        
    last_interlinking_metadata = {'origin': res.get('id'), 
                                  'interlinked_column': interlinked_column,
                                  'interlinking_reference': interlinking_reference} 
    interlinking_lineage.append(last_interlinking_metadata)
    
    # Creating a new interlinked resource
    new_res = p.toolkit.get_action('resource_create')(context,
            {
                'package_id': data_dict.get('package_id'),
                'url':'http://',
                'format':'csv',
                'name': original_name + ' (interlinked)',
                'description': 'This is an interlinked resource. It originates from `' 
                                + original_name + '` whose column `' 
                                + interlinked_column + '` was interlinked with reference dataset: `'
                                + interlinking_reference + '`',
                'interlinking_origin': res.get('id'),
                'interlinking_lineage': json.dumps(interlinking_lineage),
                'interlinked_resource': True,
                'original_file_name': original_file_name,
                'state': 'active',    
            })
    
    #Update new resource's url
    new_res['url'] = '/'.join(original_url_splitted[:3]) + "/dataset/" + data_dict.get('package_id') + '/resource/interlinking/' + new_res.get('id')
    new_res = p.toolkit.get_action('resource_update')(context, new_res)
        
    # Create a related datastore table
    # First of get all original fields
    original_ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': res.get('id')})
    fields = original_ds.get('fields')
    # Remove _id from fields list
    fields.pop(0)
    # Create new final interlinked resource with original fields
    
    new_ds = p.toolkit.get_action('datastore_create')(context,
            {
                'resource_id': new_res.get('id'),
                'force':True,
                'allow_update_with_id':True,
                'fields': fields
            })
     
    # Copying records from the original and the temporary interlinking resource to the new resource   
    #Making two lists: One with  all original columns plus the '_id' one, and one with '_id'
    columns = json.loads(p.toolkit.get_action('resource_show')(context, {'id': int_res.get('id')}).get('interlinking_columns_status'))
    original_columns = []
    interlinked_columns = []
    for col, status in columns.items():
        if col == '_id':            
            interlinked_columns.append(col)
        if status != 'not-interlinked':
            interlinked_columns.append(col)
            interlink_col_name = col
        else:
            original_columns.append(col)
            
    reference_colums = ['_id', interlinking_column]
    
    STEP = 100
    offset = 0    
    total = original_ds.get('total')
    for k in range(0,int(ceil(total/float(STEP)))):
        offset = k*STEP
        original_recs = p.toolkit.get_action('datastore_search')(context, {
                                       'resource_id':original_resource_id, 
                                       'offset': offset, 
                                       'limit': STEP, 
                                       'fields': original_columns, 
                                       'sort':'_id'}).get('records')
        interlinked_recs = p.toolkit.get_action('datastore_search')(context, {
                                       'resource_id':interlinked_resource_id, 
                                       'offset': offset, 
                                       'limit': STEP, 
                                       'fields': reference_colums, 
                                       'sort':'_id'}).get('records')
        
        #Original records are enhanced with the interlinked field values
        for orec, irec in zip(original_recs, interlinked_recs):
            orec[interlink_col_name] = irec[interlinking_column]
            
        updated_ds = p.toolkit.get_action('datastore_upsert')(context,
                {
                    'resource_id': new_res.get('id'),
                    'allow_update_with_id':True,#TOCHECK: Is it used?          
                    'force': True,
                    'records': original_recs
                    })
    # The intermediate interlinking resource is deleted along with its datastore table, and the original resource 
    # is marked as not being currently interlinked.   
    p.toolkit.get_action('interlinking_resource_delete')(context, {'resource_id': interlinked_resource_id})
    return {'interlinked_res_id': new_res.get('id')}
    

# This action provides all available reference resources for interlinking
@toolkit.side_effect_free
def interlinking_get_reference_resources(context, data_dict):
    raw_ref_resources_str = pylons.config.get('ckanext.interlinking.references')
    raw_ref_resources = raw_ref_resources_str.strip().split('\n')
    ref_resources = []
    for raw_ref_resource in raw_ref_resources:
        ref_resource_members = raw_ref_resource.split(':')
        if len(ref_resource_members) != 5:
            raise p.toolkit.ValidationError('Malformed reference resources')
        ref_resource = {}
        ref_resource['name'] = ref_resource_members[0];
        ref_resource['ref-id'] = ref_resource_members[1];
        ref_resource['dataset-id'] = ref_resource_members[2]
        ref_resource['resource-id'] = ref_resource_members[3]
        ref_resource['column-name'] = ref_resource_members[4]
        ref_resources.append(ref_resource)
    return ref_resources


@toolkit.side_effect_free
def interlinking_resource_download(context, data_dict):
    #p.toolkit.check_access('interlinking_resource_download', context, data_dict)
    schema = context.get('schema', dsschema.interlinking_resource_download_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)
    
    res_id = data_dict.get('resource_id')
    
    # Get resource to check if it is indeed an interlinking resource
    res = p.toolkit.get_action('resource_show')(context, {'id': res_id})
    if not res.get('interlinked_resource'):
        raise p.toolkit.ValidationError('Resource "{0}" is not an interlinked resource'.format(res_id))
    
    # Retrieve related datastore table
    ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': res_id})
        
    csv = unicode('')
    #Headers first
    fields = ds.get('fields')
    #Removing '_id' field
    fields = [ f for f in fields if f.get('id') != '_id' ]
        
    last = len(fields) - 1
    for i, col in enumerate(fields):
        field = col.get('id')
        if i == last:
            csv += '"' + field + '"\n'
        else:
            csv += '"' + field + '", '
            
    #Records follow
    records = ds.get('records')
    for rec in records:
        for i, f in enumerate(fields):
            if i == last:
                if f.get('type') == 'numeric':
                    csv += rec.get(f.get('id')) + '\n'
                else:
                    csv += '"' + rec.get(f.get('id')) + '"\n'
            else:
                if f.get('type') == 'numeric':
                    csv += rec.get(f.get('id')) + ', '
                else:
                    csv += '"' + rec.get(f.get('id')) + '", '
                    
    response = {}
    response['filename'] = res.get('original_file_name')
    response['csv'] = csv.encode('utf8')                
    
    return response


def interlinking_star_search(context, data_dict):
    ''' It searches lucene with a '*' wildcard. The wildcard is positioned at the end of
    the search string. 
    '''
    schema = context.get('schema', dsschema.interlinking_star_search_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)
    
    term = data_dict.get('term')
    reference_resource = data_dict.get('reference_resource')
    
    return lucene_access.search(term, reference_resource, 'like')


def interlinking_check_interlink_complete(context, data_dict):
    p.toolkit.check_access('interlinking_check_interlink_complete', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_check_interlink_complete_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)
    
    interlinked_resource_id = data_dict.get('resource_id')
    interlinking_column = data_dict.get('column_name')
    #interlinking_column = 'int__score'
    int_res = p.toolkit.get_action('resource_show')(context, {'id': interlinked_resource_id})
    filter = {interlinking_column: ''}
    ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': data_dict.get('resource_id'),
                                                            'fields': interlinking_column,
                                                            'filters': filter})    
    if ds.get('total') > 0:
        return False
    else:
        return True
    

@toolkit.side_effect_free
#TODO: remove it
def interlinking_temp(context, data_dict):
    #suggestions = lucene_access.search('Νέας Ιωνίας', 'kallikratis')
    suggestions = lucene_access.getFields('kallikratis')
        

def _initialize_columns(context, col_name, ds, total, reference_resource):
    # Get current datastore's fields
    current_fields = ds.get('fields')
    fields = current_fields
    
    # Get reference dataset's fields that should be stored in datastore
    reference_field_names = lucene_access.getFields(reference_resource, True)

    # Get fields as they supposed to be stored in the datastore
    final_fields = []
    final_fields.append({'id': reference_field_names[0], 'type': 'text'})
    final_fields.append({'id': u"int__score", 'type': 'text'})
    final_fields.append({'id': u"int__checked_flag", 'type': 'boolean'})
    final_fields.append({'id': u"int__all_results", 'type': 'text'})
    for field in reference_field_names:
        if field != reference_field_names[0]:
            final_fields.append({'id': field, 'type': 'text'})
    
    # Check that all final_fields already exist in the datastore
    datastore_recreation_needed = False
    for final_field in final_fields:
        exists = False
        for current_field in current_fields:
            if final_field['id'] == current_field['id']:
                exists = True
                break
        if exists == False:
            datastore_recreation_needed = True
            break
    
    if datastore_recreation_needed == False:
        return
    
    # Drop and recreate datastore table
    p.toolkit.get_action('datastore_delete')(context, {'resource_id': ds['resource_id'], 
                                                       'force':True})
    # Update fields with datastore_create
    new_ds = p.toolkit.get_action('datastore_create')(context,
            {
                'resource_id': ds.get('resource_id'),
                'force':True,
                'allow_update_with_id':True,
                'fields': final_fields
                #'records':[{col_name:''}]
                })
    return final_fields


def _interlink_column(context, res, col_name, original_ds, new_ds, reference, ref_fields):
    res_id = original_ds.get('resource_id')
    total = original_ds.get('total')
    columns = json.loads(res.get('interlinking_columns_status','{}'))

    # The interlinked column is marked with the reference resource with which it is interlinked.
    for k,v in columns.iteritems():
        if k == col_name:
            columns.update({k:reference})
    columns = json.dumps(columns)
    
    original_res = p.toolkit.get_action('resource_show')(context, {'id': res.get('interlinking_parent_id')})
    original_res['interlinked_column'] = col_name
    original_res = p.toolkit.get_action('resource_update')(context, original_res)
    
        
    res = p.toolkit.get_action('resource_show')(context, res)
    res['interlinking_resource'] = True
    res['interlinking_columns_status'] = columns
    res['interlinking_status'] = 'undergoing'
    res['reference_fields'] = json.dumps(ref_fields)
    res = p.toolkit.get_action('resource_update')(context, res)
    
    STEP = 100
    offset = 0
    for k in range(0,int(ceil(total/float(STEP)))):
        offset = k*STEP
        recs = p.toolkit.get_action('datastore_search')(context, {
                                        'resource_id':res_id, 
                                        'offset': offset, 
                                        'limit': STEP, 
                                        'sort':'_id'}).get('records')
        nrecs = []
        for rec in recs:
            original_term = rec.get(col_name)
            suggestions = lucene_access.search(original_term, reference, 'search')
            
            # If any suggestions were returned
            if len(suggestions['records']) > 0:
                # The first field is the field on which the search was run
                search_field = suggestions['fields'][0]
                
                if len(suggestions['records']) > 0:
                    best_suggestion = suggestions['records'][0]
                    for suggestion in suggestions['records']:
                        if suggestion['scoreField'] > best_suggestion['scoreField']:
                            best_suggestion = suggestion
                            
                    nrec = {'_id': rec.get('_id'),
                            search_field: best_suggestion[search_field],
                            'int__score': best_suggestion['scoreField'],
                            'int__checked_flag': False,
                            'int__all_results': json.dumps(suggestions)}
                    for field in suggestions['fields']:
                        if field != search_field and field != 'scoreField':
                            nrec[field] = best_suggestion[field]
                    nrecs.append(nrec)
            # No suggestions were returned         
            else:
                real_fields = lucene_access.getFields(reference, False)
                suggestions = { "fields": real_fields,
                                "records": [], 
                               }
                search_field = real_fields[0]
                nrec = {'_id': rec.get('_id'),
                            search_field: "",
                            'int__score': "",
                            'int__checked_flag': False,
                            'int__all_results': json.dumps(suggestions)}
                for field in suggestions['fields']:
                        if field != search_field and field != 'scoreField':
                            nrec[field] = ""
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
