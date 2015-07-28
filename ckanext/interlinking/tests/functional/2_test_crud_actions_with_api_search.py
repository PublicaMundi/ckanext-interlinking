# -*- encoding: utf-8 -*-

import nose.tools
import json
#import collections
#import copy
import mock
import pprint

import ckan.tests
import ckan.new_tests.factories as factories
import ckan.new_tests.helpers as helpers
import ckan.model as model
import ckan.logic as logic
import ckan.plugins as p
class TestController(ckan.tests.TestController):

    user = None

    def __init__(self):
        self.user = factories.User()

    def get_context(self):
        return {
                'model':model,
                'session':model.Session,
                'user':self.user['name'],
                'ignore_auth':True,
                'api_version':3,
                }

    ###
    ### Translate Resource Create Tests
    ###

    # If initialize_datastore or get_initial_datastore fail, all tests w.get('translation_status') == 'publishedeill fail
    @nose.tools.istest
    def test_1_initialize_dataset_and_resource(self):
        self._initialize_datastore()
        #resource_nodatastore = self._get_initial_datastore(package_data_no_datastore.get('name'))


    @nose.tools.istest
    def test_a4_create_translation_resource(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        # Should create translation resource correctly
        trans_data = [{
                'package_id': package_data.get('name'),
                'resource_id': resource.get('id'),
                'language': 'el',
                },
                {
                'package_id': package_data.get('name'),
                'resource_id': resource.get('id'),
                'language': 'es',
                }]
        for d in trans_data:
            created_res = helpers.call_action('translate_resource_create', context=context, **d)
            assert created_res.get('id')
            assert created_res.get('translation_resource')
            assert created_res.get('translation_status')
            assert created_res.get('translation_language')

    ###
    ### Translate Resource Update Tests
    ###
    @nose.tools.istest
    def test_b2_update_translation_resource(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        # Should create translation resource correctly
        res = helpers.call_action('resource_show', context=context, id=resource.get('id'))
        res_el = helpers.call_action('resource_show', context=context, id=json.loads(res.get('has_translations')).get('el'))
        assert res_el.get('id')
        assert res_el.get('translation_parent_id') == res.get('id')

        # Update same column twice to make sure no conflicts arise
        trans_data = [{
                'resource_id': res_el.get('id'),
                'mode':'manual',
                'column_name':'address',
                },
                ]

        for d in trans_data:
            helpers.call_action('translate_resource_update', context=context, **d)
            updated_ds = helpers.call_action('datastore_search', context=context, id=res_el.get('id'))
            assert updated_ds.get('resource_id') == res_el.get('id')
            assert updated_ds.get('total') == 3

    @nose.tools.nottest
    def test_b3_update_translation_resource_transcription(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()
        #context.update({'locale':'el'})
        # Should create translation resource correctly
        res = helpers.call_action('resource_show', context=context, id=resource.get('id'))
        res_el = helpers.call_action('resource_show', context=context, id=json.loads(res.get('has_translations')).get('el'))
        assert res_el.get('id')
        assert res_el.get('translation_parent_id') == res.get('id')

        trans_data = [{
                'resource_id': res_el.get('id'),
                'mode':'transcription',
                'column_name':'name',
                },
                {
                'resource_id': res_el.get('id'),
                'mode':'manual',
                'column_name':'name',
                },
                {
                'resource_id': res_el.get('id'),
                'mode':'transcription',
                'column_name':'address',
                }]

        for d in trans_data:
            helpers.call_action('translate_resource_update', context=context, **d)
            updated_ds = helpers.call_action('datastore_search', context=context, id=res_el.get('id'))
            pprint.pprint(updated_ds)

            assert updated_ds.get('resource_id') == res_el.get('id')
            assert updated_ds.get('total') == 3
            field_exists = False
            for field in updated_ds.get('fields'):
                if field.get('id') == d.get('column_name'):
                    field_exists = True
                    break
            assert field_exists

        res = helpers.call_action('datastore_search', context=context, id=res_el.get('id'))
        for rec in res.get('records'):
            print rec
            assert rec.get('name') == None
        pprint.pprint(res)
        assert rec.get('address')

    @nose.tools.istest
    def test_d1_publish_translation_resource(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        # Should create translation resource correctly
        res = helpers.call_action('resource_show', context=context, id=resource.get('id'))
        res_el = helpers.call_action('resource_show', context=context, id=json.loads(res.get('has_translations')).get('el'))
        assert res_el.get('id')
        assert res_el.get('translation_resource')

        trans_data = [{
                'resource_id': res_el.get('id'),
                },
                ]

        for d in trans_data:
            helpers.call_action('translate_resource_publish', context=context, **d)
            published_res = helpers.call_action('resource_show', context=context, id=res_el.get('id'))
            assert published_res.get('translation_status') == 'published'

    @nose.tools.istest
    def test_e1_search_translation_resource(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        # Should create translation resource correctly
        res = helpers.call_action('resource_show', context=context, id=resource.get('id'))
        res_el = helpers.call_action('resource_show', context=context, id=json.loads(res.get('has_translations')).get('el'))
        assert res_el.get('id')
        assert res_el.get('translation_parent_id') == res.get('id')

        trans_data = {
                'resource_id': res.get('id'),
                'language': 'el'
                }
        pprint.pprint(helpers.call_action('translate_resource_search', context=context, **trans_data))
        asd123

        # Helpers
    def _get_initial_datastore(self, name):
        context = self.get_context()
        print 'name'
        print name
        res = helpers.call_action('package_show', context=context, id=name)
        # Assert resource exists and return it
        assert res.get('id')
        assert res.get('resources')[0]
        return res.get('resources')[0]

    def _initialize_datastore(self):
        context = self.get_context()
        # Step 1 - create package
        package = helpers.call_action('package_create', context=context, **package_data_no_datastore)
        created_package = helpers.call_action('package_show', context=context, id=package_data_no_datastore.get('name'))
        print 'package_no_datastore'
        assert created_package
        assert created_package.get('id') == package.get('id')
        assert created_package.get('resources')
        assert created_package.get('resources')[0].get('id')

        package = helpers.call_action('package_create', context=context, **package_data)
        assert helpers.call_action('package_show', context=context, id=package.get('id')).get('id')
        # Step 2 - create the resource and table and fill it with sample data
        datastore = helpers.call_action('datastore_create', context=context, **datastore_data)
        # TODO: auth checks - not working
        #package = helpers.call_action('package_update', context=context, id=package['id'], name='changed')
        # Assert package created correctly
        created_package = helpers.call_action('package_show', context=context, id=package.get('id'))
        print 'package'
        # Assert resource created
        assert helpers.call_action('resource_show', context=context, id=datastore.get('resource_id')).get('id')
        assert created_package.get('resources')
        assert created_package.get('resources')[0].get('id')

        # Assert datastore created
        res = helpers.call_action('datastore_search', context=context, resource_id=datastore.get('resource_id'))
        assert res.get('resource_id') == datastore.get('resource_id')

# Initial test data
package_data = {
            'name': 'hello-ckan-2',
            'title': u'Hello Ckan 2',
        }

package_data_no_datastore= {
            'name': 'hello-ckan-1',
            'title': u'Hello Ckan 1',
            'resources':[{'package_id': 'hello-ckan-1',
                        'url':'http://',
                        'url_type':'datastore',
                        'name':'hello-resource-1',
                        'format':'data_table'
                    }],
        }

datastore_data = {
                #'resource_id':package.get('resources')[0].get('id'),
                'force': True,
                'resource': {'package_id': 'hello-ckan-2',
                    },
                'fields':[{'id':'name',
                        'type':'text'},
                        {'id':'address',
                        'type':'text'},
                        {'id':'post_code',
                        'type':'text'}],
                'records':[{'name':u'Δημήτρης Γιαννακόπουλος',
                        'address':u'Αγ. Τράκη 56, Μαρούσι',
                        'post_code':'131313'},
                        {'name':u'Αχιλλέας Μπέος',
                        'address':u'Ιωαννίνων 5, Βόλος',
                        'post_code':'12345'},
                        {'name':u'Τάκης Τσουκαλάς',
                        'address':u'Κάδος Πατησίων 55, Πατήσια',
                        'post_code':'41235'}]
                }

