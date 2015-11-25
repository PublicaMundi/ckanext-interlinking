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
                'interlinking_columns_status':columns_status
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
    if isinstance(ref_fields, int) and ref_fields == -1:
        raise p.toolkit.ValidationError('Internal Server Error occurred')
    res = _interlink_column(context, res, col_name, original_ds, ds, reference_resource, ref_fields)
    if isinstance(res, int) and ref_fields == -1:
        raise p.toolkit.ValidationError('Internal Server Error occurred')
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
    if upd_original_res.get('interlinked_column'):
        del upd_original_res['interlinked_column']
    upd_original_res = p.toolkit.get_action('resource_update')(context, upd_original_res)
    
    return p.toolkit.get_action('resource_delete')(context, {'id': data_dict.get('resource_id')})


def interlinking_resource_finalize(context, data_dict):
    '''Finalizes the interlinked resource, i.e. a new one is created where the original interlinked column 
    is being replaced by the new interlinked one.
    '''
    p.toolkit.check_access('interlinking_resource_finalize', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_resource_finalize_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)

    interlinked_resource_id = data_dict.get('resource_id')
    int_res = p.toolkit.get_action('resource_show')(context, {'id': interlinked_resource_id})
    # If the original resource is already interlinked
    if not int_res.get('reference_fields'):
        raise p.toolkit.ValidationError('Resource "{0}" is not been interlinked yet. ' \
                        'Thus it cannot be finalized'.format(int_res.get('interlinking_parent_id')))
    
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
    interlinking_column = json.loads(int_res.get('reference_fields'))[0]['id']
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








@logic.side_effect_free
def interlinking_resource_search(context, data_dict):
    '''Provides a complete interlinking resource providing fields from both the interlinked resource
    and the interlinking temporary one. It gets as parameters the resource_id and datastore_search filters'''
        
    schema = context.get('schema', dsschema.interlinking_resource_search_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)
    
    orig_res = p.toolkit.get_action('resource_show')(context, {'id': data_dict.get('resource_id')})
    if not orig_res.get('on_interlinking_process'):
        raise p.toolkit.ValidationError('Resource "{0}" is not under interlinking process'.format(orig_res.get('id')))
    
    int_res = p.toolkit.get_action('resource_show')(context, {'id': orig_res.get('temp_interlinking_resource')})
    ori_ds = p.toolkit.get_action('datastore_search')(context, {'id': data_dict.get('resource_id')})
    int_ds = p.toolkit.get_action('datastore_search')(context, {'id': orig_res.get('temp_interlinking_resource')})
    
    original_fields = [x['id'] for x in ori_ds.get('fields')]
    interlinking_fields = [x['id'] for x in int_ds.get('fields')]    
    # Checking if the original and the interlinking resources have common column names apart 
    #     from '_id. In that case their column names have to be namespaced
    common_field_names_exist = bool(set(original_fields).intersection(set(interlinking_fields) - set(['_id'])))
    if data_dict.get('fields'):
        for field in _get_list(data_dict.get('fields')):
            if not (field in original_fields or field in interlinking_fields):
                raise p.toolkit.ValidationError(u'Requested field {0} does not exist in either resource\'s {1} '
                                                'nor resource\'s {2} tables'.format(field, 
                                                                                    data_dict.get('resource_id'), 
                                                                                    int_res.get('interlinking_parent_id')))
    interlinking_columns_status = int_res.get('interlinking_columns_status')
    
    data_dict.update({'original_fields': original_fields, 
                      'interlinking_fields': interlinking_fields, 
                      'interlinking_resource_id': int_res.get('id'), 
                      'interlinking_columns_status': interlinking_columns_status})
    
    create_view_results = _create_view(context, data_dict)
    
    data = {'sql': create_view_results.get('sql'), 'fields_status': create_view_results.get('fields_status')}
    ds_search_sql = p.toolkit.get_action('datastore_search_sql')(context, data)    
    params_dict = _get_params_dict(data_dict)
    ds_search_sql.update(params_dict)
    
    # if after all fields have to be namespaced
    if common_field_names_exist:
        temp_fields = ds_search_sql['fields']
        temp_fields = [_namespace_fields(f, original_fields, orig_res.get('id'), int_res.get('id')) for f in temp_fields]
        ds_search_sql['fields'] = temp_fields;
        
        temp_records = ds_search_sql['records']
        temp_records = [_namespace_record(r, original_fields, orig_res.get('id'), int_res.get('id')) for r in temp_records]
        ds_search_sql['records'] = temp_records;
        
        temp_column_status = ds_search_sql['fields_status']
        temp_column_status = {_namespace_simple_field(k, original_fields, orig_res.get('id'), int_res.get('id')): 
                              temp_column_status[k] for k in temp_column_status}
        ds_search_sql['fields_status'] = temp_column_status
    return ds_search_sql



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
    
    # Get resource to check if it is indeed an interlinking resourceinterlinking_column = json.loads(int_res.get('reference_fields'))[0]['id']
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
    ''' It searches lucene with a '*' wildcard. The wildcard is positioned at the end of \
    the search string. 
    '''
    schema = context.get('schema', dsschema.interlinking_star_search_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)
    
    term = data_dict.get('term')
    reference_resource = data_dict.get('reference_resource')
    
    terms = lucene_access.search(term, reference_resource, 'like')
    if isinstance(terms, int):
        return ''
    
    return terms


def interlinking_check_interlink_complete(context, data_dict):
    p.toolkit.check_access('interlinking_check_interlink_complete', context, data_dict)

    schema = context.get('schema', dsschema.interlinking_check_interlink_complete_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)
    
    interlinked_resource_id = data_dict.get('resource_id')
    int_res = p.toolkit.get_action('resource_show')(context, {'id': interlinked_resource_id})
    # If the original resource is already interlinked
    if not int_res.get('reference_fields'):
        return -2
    interlinking_column = json.loads(int_res.get('reference_fields'))[0]['id']
    filter = {interlinking_column: ''}
    ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': data_dict.get('resource_id'),
                                                            'fields': interlinking_column,
                                                            'filters': filter})    
    if ds.get('total') > 0:
        return -1
    else:
        return 0
    
    
def interlinking_apply_to_all(context, data_dict):
    p.toolkit.check_access('interlinking_apply_to_all', context, data_dict)
    
    schema = context.get('schema', dsschema.interlinking_apply_to_all_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
        raise p.toolkit.ValidationError(errors)
    
    
    interlinked_resource_id = data_dict.get('resource_id')
    int_res = p.toolkit.get_action('resource_show')(context, {'id': interlinked_resource_id})
    
    # If the original resource is not yet interlinked
    if not int_res.get('reference_fields'):
        raise p.toolkit.ValidationError('Resource "{0}" is not been interlinked yet. ' \
                        'Thus it cannot be finalized'.format(int_res.get('interlinking_parent_id')))
    
    orig_ds = p.toolkit.get_action('datastore_search')(context, {'resource_id': int_res.get('interlinking_parent_id')})
    row_id = data_dict.get('row_id')
    original_column_name = [k for (k, v) in json.loads(int_res.get('interlinking_columns_status')).iteritems() if v != 'not-interlinked'][0]
    interlinked_column_name = json.loads(int_res.get('reference_fields'))[0].get('id')
    
    reference_row = p.toolkit.get_action('datastore_search')(context, {
                                       'resource_id': int_res.get('id'), 
                                       'filters': {u'_id': row_id}}).get('records')[0]
                                                                              
    original_value = p.toolkit.get_action('datastore_search')(context, {
                                       'resource_id': int_res.get('interlinking_parent_id'), 
                                       'fields': [original_column_name],
                                       'filters': {u'_id': row_id}}).get('records')[0].get(original_column_name)
                                                                                  
    # It will carry all fields which has to be updated except 'int__all_results' which is treated separately
    updatable_fields = [u'int__score', u'int__checked_flag']
    all_result_fields = json.loads(reference_row.get('int__all_results')).get('fields') 
    for field in all_result_fields:
        if field != 'scoreField':
            updatable_fields.append(field)
            
    reference_values = {}
    for field in updatable_fields:
        reference_values[field] = reference_row.get(field)  

    interlinked_value = reference_values[interlinked_column_name]
            
    STEP = 100
    offset = 0    
    total = orig_ds.get('total')                        
    for k in range(0,int(ceil(total/float(STEP)))):
        offset = k*STEP
        original_recs = p.toolkit.get_action('datastore_search')(context, {
                                       'resource_id': int_res.get('interlinking_parent_id'), 
                                       'offset': offset, 
                                       'limit': STEP, 
                                       'fields': [u'_id', original_column_name],
                                       'sort':'_id'}).get('records')
        interlinked_recs = p.toolkit.get_action('datastore_search')(context, {
                                       'resource_id':int_res.get('id'), 
                                       'offset': offset, 
                                       'limit': STEP, 
                                       'sort':'_id'}).get('records') 
                                       
        updatable_recs = []
        for orec, irec in zip(original_recs, interlinked_recs):
            if orec.get(original_column_name) == original_value:
                for field in updatable_fields:
                    irec[field] = reference_values[field]
                # Updating 'int__all_results' part
                interlinked_field_values = [rec.get(interlinked_column_name) for rec in json.loads(irec['int__all_results']).get('records')]
                all_result_fields = json.loads(irec['int__all_results']).get('fields')
                if interlinked_value not in interlinked_field_values:
                    new_res_rec = {interlinked_column_name: interlinked_value}
                    new_res_rec[u'scoreField'] = reference_values[u'int__score']
                    for field in all_result_fields:
                        if field != interlinked_column_name and field != 'scoreField':
                            new_res_rec[field] = reference_values[field]
                    new_res_recs = json.loads(irec['int__all_results']).get('records')
                    new_res_recs.append(new_res_rec)
                    irec['int__all_results'] = json.dumps({'fields': all_result_fields, 'records': new_res_recs})
                updatable_recs.append(irec)
                    
        updated_ds = p.toolkit.get_action('datastore_upsert')(context,
        {
            'resource_id': int_res.get('id'),
            'allow_update_with_id':True,#TOCHECK: Is it used?          
            'force': True,
            'records': updatable_recs
            })
    return
                    


def _initialize_columns(context, col_name, ds, total, reference_resource):
    # Get current datastore's fields
    current_fields = ds.get('fields')
    fields = current_fields
    
    # Get reference dataset's fields that should be stored in datastore
    reference_field_names = lucene_access.getFields(reference_resource, True)
    if isinstance(reference_field_names, list):
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
    else:
        # It carries -1 value as an error code
        return reference_field_names


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
            
            if isinstance(suggestions, int):
                return -1
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
                if isinstance(real_fields, list):
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
                else:
                    return -1
                
            
        ds = p.toolkit.get_action('datastore_upsert')(context,
                {
                    'resource_id': new_ds.get('resource_id'),
                    'allow_update_with_id':True,
                    'force': True,
                    'records': nrecs
                    })
          
        offset=offset+STEP
    return new_ds




def _get_params_dict(data_dict):
    params_dict = {}
    for param in ['limit', 'offset', 'fields', 'sort', 'filters', 'q']:
        if param in data_dict:
            params_dict[param] = data_dict.get(param)
    return params_dict


def _create_view(context, data_dict):
    '''Creates a view combination fields from the interlinked resource 
    and the temporary interlinking one.'''
    
    orig_resource = data_dict.get('resource_id')
    inter_resource = data_dict.get('interlinking_resource_id')

    original_fields = data_dict.get('original_fields',[])
    interlinking_fields = data_dict.get('interlinking_fields',[])
    selected_fields = data_dict.get('fields')#, original_fields)
    if not selected_fields is None:
        data_dict.update({'fields_selected': True})
    else:
        selected_fields = original_fields
    field_ids = _get_fields(selected_fields, data_dict)
    field_tupples = [(f.split('.')[1].strip('"'), f.split('.')[0].strip('"')) for f in field_ids]
    fields_status = {}
    interlinking_field_count = 0
    original_interlinked = None
    for field_tuple in field_tupples:
        if field_tuple[1] == orig_resource:
            fields_status[field_tuple[0]] = 'original'
            original_interlinked = field_tuple[0]
        elif field_tuple[1] == inter_resource:
            if interlinking_field_count == 0:
                fields_status[field_tuple[0]] = 'interlinking_result'
                fields_status[original_interlinked] = 'orignal_interlinked'
            elif interlinking_field_count == 1:
                fields_status[field_tuple[0]] = 'interlinking_score'
            elif interlinking_field_count == 2:
                fields_status[field_tuple[0]] = 'interlinking_check_flag'
            elif interlinking_field_count == 3:
                fields_status[field_tuple[0]] = 'interlinking_all_results'
            else:
                fields_status[field_tuple[0]] = 'reference_auxiliary'
            interlinking_field_count += 1
    
    sql_fields = u", ".join(field_ids)
    limit = data_dict.get('limit', 100)
    offset = data_dict.get('offset', 0)
    
    combined_fields = original_fields + list(set(interlinking_fields) - set (original_fields))
    sort = _sort(context, data_dict, combined_fields)    
    sql_string = u'''SELECT {fields}, \
                    COUNT (*) OVER () AS "_full_count" \
                    FROM "{orig_resource}" \
                    LEFT JOIN "{inter_resource}" \
                    ON "{orig_resource}"._id = "{inter_resource}"._id \
                    {sort} \
                    OFFSET {offset} \
                    LIMIT {limit};'''.format(fields = sql_fields,
                                             orig_resource = orig_resource,
                                             inter_resource = inter_resource,
                                             sort = sort,
                                             offset = offset,
                                             limit = limit)
                    
     
    return {'sql': sql_string.encode('utf-8'), 'fields_status': fields_status}
    

def _get_fields(fields, data_dict):
    #orig_field_ids = data_dict.get('fields', [])
    inter_field_ids = data_dict.get('interlinking_fields', [])
    orig_table = data_dict.get('resource_id')
    inter_table = data_dict.get('interlinking_resource_id')

    all_field_ids = _get_list(fields)
    interlinking_columns_status = json.loads(data_dict.get('interlinking_columns_status'))
    field_ids = []
    
    # If fields are selected by the user
    if data_dict.get('fields_selected'):
        for field in all_field_ids:
            if field in inter_field_ids and not field == '_id':
                table = inter_table
            else:
                table = orig_table
            # if column translation available rename field using alias
            field_ids.append(u'"{0}"."{1}"'.format(table, field))
    else:
        for field in all_field_ids:
            field_ids.append(u'"{0}"."{1}"'.format(orig_table, field))
            if not interlinking_columns_status.get(field) == 'not-interlinked' and not interlinking_columns_status.get(field) == '':
                for int_field in inter_field_ids:
                    if int_field != '_id':
                        field_ids.append(u'"{0}"."{1}"'.format(inter_table, int_field))
    return field_ids


def _get_list(input, strip=True):
    '''Transforms a string or list to a list'''
    if input is None:
        return
    if input == '':
        return []

    l = converters.aslist(input, ',', True)
    if strip:
        return [_strip(x) for x in l]
    else:
        return l
    

def _sort(context, data_dict, field_ids):
    sort = data_dict.get('sort')
    if not sort:
        if data_dict.get('q'):
            return u'ORDER BY rank'
        else:
            return u''

    resource_id = data_dict.get('resource_id')
    clauses = _get_list(sort, False)

    clause_parsed = []
    for clause in clauses:
        clause = clause.encode('utf-8')
        clause_parts = shlex.split(clause)
        if len(clause_parts) == 1:
            table, field, sort = '', clause_parts[0], 'asc'
        elif len(clause_parts) == 2:
            table, field, sort = '', clause_parts[0], clause_parts[1]
        elif len(clause_parts) == 3:
            table, field, sort = clause_parts
        else:
            raise ValidationError({
                'sort': ['not valid syntax for sort clause']
            })
        field, sort = unicode(field, 'utf-8'), unicode(sort, 'utf-8')

        if field not in field_ids:
            raise ValidationError({
                'sort': [u'field "{0}" not in table with fields {1}'.format(
                    field, field_ids)]
            })
        if sort.lower() not in ('asc', 'desc'):
            raise ValidationError({
                'sort': ['sorting can only be asc or desc']
            })
        if table != '':
            clause_parsed.append(u'"{0}"."{1}" {2}'.format(
                table, field, sort)
            )
        else:
            clause_parsed.append(u'"{0}" {1}'.format(
                field, sort)
            )

    if clause_parsed:
        return "order by " + ", ".join(clause_parsed)
    
    
def _strip(input):
    if isinstance(input, basestring) and len(input) and input[0] == input[-1]:
        return input.strip().strip('"')
    return input


def _namespace_record (record_dict, original_fields, original_resource_id, interlinking_resource_id):
    return {_namespace_simple_field(k, original_fields, original_resource_id, interlinking_resource_id): record_dict[k]
            for k in record_dict}
    

def _namespace_fields (field_dict, original_fields, original_resource_id, interlinking_resource_id):
    return {'id': _namespace_simple_field(field_dict['id'], original_fields, original_resource_id, interlinking_resource_id),
                'type': field_dict['type']}
    
    
def _namespace_simple_field (field_id, original_fields, original_resource_id, interlinking_resource_id):
    if field_id in original_fields:
        table_id = original_resource_id
    else:
        table_id = interlinking_resource_id
    return table_id + '.' + field_id


"""
def _namespace_record (record_dict, original_fields, original_resource_id, interlinking_resource_id):
    return {_namespace_simple_field(k, original_fields, original_resource_id, interlinking_resource_id): (record_dict[k]
            if k != 'int__all_results' else _namespace_field__int_all_results(record_dict[k], interlinking_resource_id))
            for k in record_dict}
    
def _namespace_field__int_all_results(int_all_results_str, interlinking_resource_id):
    int_all_results = json.loads(int_all_results_str)
    fields = int_all_results.get('fields')
    records = int_all_results.get('records')
    fields = [interlinking_resource_id + '.' + f if f != 'scoreField' else f for f in fields]
    records = [_namespace_record__int_all_results(r, interlinking_resource_id) for r in records]
    return json.dumps({'fields': fields, 'records': records})

def _namespace_record__int_all_results(rec, interlinking_resource_id):
    return {(interlinking_resource_id + '.' + k if k != 'scoreField' else k): rec[k] for k in rec}
"""
