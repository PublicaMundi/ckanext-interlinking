from logging import getLogger

import ckan.plugins as p
import ckan.plugins.toolkit as toolkit
from ckan.common import request
from routes.mapper import SubMapper
import ckan.model as model

import ckanext.interlinking.logic.action as action
import ckanext.interlinking.logic.auth as auth
import ckanext.interlinking.controllers.package as package_controller
import ckan.lib.i18n as i18n

import pprint
import json
log = getLogger(__name__)


class ReclinePreviewInterlinking(p.SingletonPlugin):
    """This extension previews resources using recline for interlinking purposes

        This extension implements several interfaces

      - ``IConfigurer`` to modify the configuration
      - ``IActions`` to define interlinking-specific API actions
      - ``IRoutes`` to create mapping to the required controllers
      - ``IAuthFunctions`` to define auth functions for the interlinking-specific API actions
      - ``ITemplateHelpers`` to define new template helpers

        Important note: For the moment since there is not way to select better preview, if recline preview is enabled recline-interlinking plugin
                        must be loaded with higher priority in development.ini
    """
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IAuthFunctions)
    p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)
    #p.implements(p.IResourcePreview, inherit=True)


    # IConfigurer 1/1 function (inherit=True)
    def update_config(self, config):
        ''' Set up the resource library, public directory and
        template directory for the preview
        '''
        toolkit.add_public_directory(config, 'theme/public')
        toolkit.add_template_directory(config, 'theme/templates')
        toolkit.add_resource('theme/public', 'ckanext-interlinking')
        toolkit.add_resource('theme/public/vendor/slickgrid', 'ckanext-interlinking-slickgrid')
    
    # IActions 1/1 function 
    def get_actions(self):
        return {
                'interlinking_resource_create': action.interlinking_resource_create,
                'interlinking_resource_update': action.interlinking_resource_update,
                'interlinking_resource_delete': action.interlinking_resource_delete,
                'interlinking_resource_finalize': action.interlinking_resource_finalize,
                'interlinking_get_reference_resources': action.interlinking_get_reference_resources,
                'interlinking_resource_download': action.interlinking_resource_download,
                'interlinking_check_interlink_complete': action.interlinking_check_interlink_complete,
                'interlinking_apply_to_all': action.interlinking_apply_to_all,
                #TODO: remove it
                'interlinking_temp': action.interlinking_temp,
                'interlinking_star_search': action.interlinking_star_search,
                }
    
    # IRoutes 1/1 function (inherit=True)
    def before_map(self, mapper):
        mapper.connect(
                'resource_interlink',
                '/dataset/{id}/resource_interlink/{resource_id}',
                controller='ckanext.interlinking.controllers.package:InterlinkingController',
                action = 'resource_interlink')

        mapper.connect(
                'resource_interlink_inner',
                '/dataset/{id}/resource_interlink_inner/{resource_id}',
                controller='ckanext.interlinking.controllers.package:InterlinkingController',
                action = 'resource_datapreview')
        
        mapper.connect(
                       'interlinking_resource_download',
                       '/dataset/{id}/resource/interlinking/resource/{resource_id}',
                       controller = 'ckanext.interlinking.controllers.package:InterlinkingController',
                       action = 'interlinking_resource_download')
        return mapper
    
    #IPackageController 1/1 function (inherit=True)
    def before_view(self, data_dict):
        # TODO: Need to cut intermediate interlinking resources here
        # so they are not visible in UI/other API functions
        for k,v in data_dict.iteritems():
            pass
            
            if k=='resources':
                new_res = []
                for res in v:
                    if not 'interlinking_resource' in res:
                        new_res.append(res)
                data_dict.update({k:new_res}) 
        data_dict.update({'num_resources':len(new_res)})
        return data_dict

    # IAuthFunctions 1/1 function
    def get_auth_functions(self):
        return {
                'interlinking_resource_create': auth.interlinking_resource_create,
                'interlinking_resource_update': auth.interlinking_resource_update,
                'interlinking_resource_delete': auth.interlinking_resource_delete,
                'interlinking_resource_finalize': auth.interlinking_resource_finalize,
                'interlinking_check_interlink_complete': auth.interlinking_check_interlink_complete,
                'interlinking_get_reference_resources': auth.interlinking_get_reference_resources,
                'interlinking_apply_to_all': auth.interlinking_apply_to_all,
                }
    # ITemplateHelpers 1/1 function    
    def get_helpers(self):
        return {
                #'resource_edit': resource_edit,
                }
        
    #IResourcePreview
    """
    def can_preview(self, data_dict):
        # if the resource is in the datastore then we can preview it with recline
        if data_dict['resource'].get('datastore_active'):
            return True
        format_lower = data_dict['resource']['format'].lower()
        previewable = format_lower in ['csv', 'xls', 'tsv']
        return {
                'can_preview': previewable,
                'quality': 3
                }
    """
    def _get_context(self):
        return {
                'model':model,
                'session':model.Session,
                'ignore_auth':True,
                'api_version':3,
                }
