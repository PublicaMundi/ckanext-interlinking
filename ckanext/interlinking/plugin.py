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
    """This extension previews resources using recline

        This extension implements two interfaces

      - ``IConfigurer`` allows to modify the configuration

        Important note: For the moment since there is not way to select better preview, if recline preview is enabled recline-interlinking plugin
                        must be loaded with higher priority in development.ini
    """
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IAuthFunctions)
    #p.implements(p.IPackageController, inherit=True)
    p.implements(p.ITemplateHelpers)

    def update_config(self, config):
        ''' Set up the resource library, public directory and
        template directory for the preview
        '''
        toolkit.add_public_directory(config, 'theme/public')
        toolkit.add_template_directory(config, 'theme/templates')
        toolkit.add_resource('theme/public', 'ckanext-interlinking')

    def get_helpers(self):
        return {
                #'resource_edit': resource_edit,
                }

    def before_map(self, mapper):
        mapper.connect(
                'resource_interlink',
                '/dataset/{id}/resource_interlink/{resource_id}',
                controller='ckanext.interlinking.controllers.package:UserController',
                action = 'resource_interlink')

        mapper.connect(
                'resource_interlink_inner',
                '/dataset/{id}/resource_interlink_inner/{resource_id}',
                controller='ckanext.interlinking.controllers.package:UserController',
                action = 'resource_datapreview')

        return mapper

    def after_update(self, context, data_dict):
        return data_dict
        
    def before_show(self, resource_dict):
        return resource_dict
    #def after_update(self, context, data_dict):
    #    print 'BEFORE UPDATE'
    #    resources = data_dict.get('resources')
    #    new_resources = copy.deepcopy(resources)
    #    for res in resources:
    #        translations = json.loads(res.get('has_translations', u'{}'))
    #        for trans,id in translations.iteritems():
    #            res = p.toolkit.get_action('resource_show')(context, {'id':id})
    #            new_resources.append(res)
    #    data_dict.update({'resources':new_resources, 'num_resources':len(new_resources)})

    def after_delete(self, context, data_dict):
        print 'AFTER DELETE'
    
    def after_show(self, context, data_dict):
        return data_dict
        #for k,v in data_dict.iteritems():
        #    if k=='resources':
        #        new_res = []
        #        for res in v:
        #            if not (('translation_resource' in res) and res.get('translation_status')=='published'):
        #                new_res.append(res)
        #        data_dict.update({k:new_res})
        #data_dict.update({'num_resources':len(new_res)})

    def before_view(self, data_dict):
        # TODO: Need to cut extra translation resources here
        # so they are not visible in UI/other API functions
        #return data_dict
        return data_dict

    def get_actions(self):
        return {
                'interlinking_resource_create': action.interlinking_resource_create,
                'interlinking_resource_update': action.interlinking_resource_update,
                'interlinking_resource_delete': action.interlinking_resource_delete,
                'interlinking_resource_publish': action.interlinking_resource_publish,
                }

    def _get_context(self):
        return {
                'model':model,
                'session':model.Session,
                'ignore_auth':True,
                'api_version':3,
                }

    def get_language_translation_status(self, res, lang):
        orig_lang = self.get_orig_language(res)
        translations = json.loads(res.get('has_translations', '{}'))
        lang = unicode(lang)
        if lang not in translations.keys():
            return 'none'
        context = self._get_context()
        trans_id = translations[lang]
        trans_res = toolkit.get_action('resource_show')(context, {'id':trans_id})
        return trans_res.get('translation_status')

    def get_resource_languages(self, res):
        orig_lang = self.get_orig_language(res)
        if not orig_lang:
            return None
        #return 
        #json.loads(res.get('has_translations','u{'+orig_lang+'}'))
        translations = json.loads(res.get('has_translations', '{}'))
        context = self._get_context()
        published_langs = [orig_lang]
        for lang,id in translations.iteritems():
            trans_res = toolkit.get_action('resource_show')(context, {'id':id})
            print 'trans res'
            print trans_res
            if trans_res.get('translation_status') == 'published':
                published_langs.append(lang)
        return published_langs

    def _decide_language(self, res):
        # Have to decide original resource language
        return 'en'

    def set_orig_language(self, res):
        context = self._get_context()
        #### Make decision
        language = self._decide_language(res, pkg)
        data = {'id':res.get('id'), 'resource_language':language, 'format':res.get('format') }
        return p.toolkit.get_action('resource_update')(context, data)

    def get_orig_language(self, res):
        available_locales = i18n.get_available_locales()
        orig_lang = res.get('resource_language', self._decide_language(res))
        for locale in available_locales:
            if locale == orig_lang:
                return locale

    def get_auth_functions(self):
        return {
                'interlinking_resource_create': auth.interlinking_resource_create,
                'interlinking_resource_update': auth.interlinking_resource_update,
                'interlinking_resource_delete': auth.interlinking_resource_delete,
                'interlinking_resource_publish': auth.interlinking_resource_publish,
                }
