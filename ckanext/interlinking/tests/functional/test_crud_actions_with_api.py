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

    # If initialize_datastore or get_initial_datastore fail, all tests will fail
    @nose.tools.istest
    def test_1_initialize_dataset_and_resource(self):
        self._initialize_datastore()
        #resource_nodatastore = self._get_initial_datastore(package_data_no_datastore.get('name'))

    @nose.tools.istest
    def test_a2_create_translation_resource_noauth(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()
        context.update({'ignore_auth':False})

        nose.tools.assert_raises(logic.NotAuthorized, helpers.call_action, 'translate_resource_create', context=context)


    @nose.tools.istest
    def test_a3_create_translation_resource_invalid(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        # Should raise validation error on all
        incomplete_or_wrong_trans_data = [{
                    'package_id': package_data.get('name'),
                    'resource_id': resource.get('id'),
                    },
                    {
                    'package_id': package_data.get('name'),
                    'resource_id': resource.get('id'),
                    'language': 'gr',
                    },
                    {
                    'package_id': package_data.get('name'),
                    'language': 'gr',
                    },
                    {
                    'package_id': package_data.get('name'),
                    'language':'el',
                    },
                    {
                    'package_id': package_data.get('name'),
                    'resource_id': 'non-existing-resource-id',
                    'language': 'el',
                    },
                    {
                    'package_id': 'non-existing package name',
                    'resource_id': resource.get('id'),
                    'language': 'el',
                    }
                    ]

        for d in incomplete_or_wrong_trans_data:
            nose.tools.assert_raises(p.toolkit.ValidationError,
                                    helpers.call_action, 'translate_resource_create', context = context, **d)

        #resource_nodatastore = self._get_initial_datastore(package_data_no_datastore.get('name'))
        #pprint.pprint(resource_nodatastore)
        # Should raise not found error on all

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

    @nose.tools.istest
    def test_a5_create_same_translation_resource(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        # Should create translation resource correctly
        trans_data = {
                'package_id': package_data.get('name'),
                'resource_id': resource.get('id'),
                'language': 'el',
                }

        nose.tools.assert_raises(p.toolkit.ValidationError, helpers.call_action, 'translate_resource_create', context=context, **trans_data)

    ###
    ### Translate Resource Update Tests
    ###
    @nose.tools.istest
    def test_b1_update_translation_resource_invalid(self):
        context = self.get_context()
        resource = self._get_initial_datastore(package_data.get('name'))
        res_el = helpers.call_action('resource_show', context=context, id=json.loads(resource.get('has_translations')).get('el'))

        # Should create translation resource correctly
        trans_data = [
                {
                'resource_id': 'wrong-resource-id',
                'mode':'manual',
                'column_name': 'address',
                },
                {
                # parent resource id
                'resource_id': resource.get('id'),
                'mode':'manual',
                'column_name': 'address',
                },

                {
                'resource_id': res_el.get('id'),
                'mode':'no-such-mode',
                'column_name': 'address',
                },
                {
                'resource_id': res_el.get('id'),
                'mode':'manual',
                'column_name': 'wrong-column-name',
                }]

        for d in trans_data:
            nose.tools.assert_raises(p.toolkit.ValidationError, helpers.call_action, 'translate_resource_update', context=context, **d)
            #pprint.pprint(helpers.call_action ('translate_resource_update', context=context, **d))

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
                {
                'resource_id': res_el.get('id'),
                'mode':'manual',
                'column_name':'address',
                }]

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
    def test_b4_update_translation_resource_transcription_pagination(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()
        #context.update({'locale':'el'})
        # Should create translation resource correctly
        res = helpers.call_action('resource_show', context=context, id=resource.get('id'))
        res_el = helpers.call_action('resource_show', context=context, id=json.loads(res.get('has_translations')).get('el'))

        assert res_el.get('id')
        assert res_el.get('translation_parent_id') == res.get('id')

        original_ds = helpers.call_action('datastore_search', context=context, id=res.get('id'))
        # Update ds with 5000 dummy name entries
        p.toolkit.get_action('datastore_upsert')(context,
            {
                'resource_id': original_ds.get('resource_id'),
                'force':True,
                'method':'insert',
                'allow_update_with_id':True,
                'records': [{'name':'λαλαλα'} for i in range(1,4998)]
            })

        ds = helpers.call_action('datastore_search', context=context, id=res.get('id'))
        print 'added?'
        #pprint.pprint(ds)
        assert ds.get('resource_id') == res.get('id')
        assert ds.get('total') == 5000

        trans_data = [{
                'resource_id': res_el.get('id'),
                'mode':'transcription',
                'column_name':'name',
                },
                #{
                #'resource_id': res_el.get('id'),
                #'mode':'manual',
                #'column_name':'name',
                #},
                #{
                #'resource_id': res_el.get('id'),
                #'mode':'transcription',
                #'column_name':'address',
                #}
        ]

        for d in trans_data:
            helpers.call_action('translate_resource_update', context=context, **d)
            updated_ds = helpers.call_action('datastore_search', context=context, id=res_el.get('id'))
            #pprint.pprint(updated_ds)

            #assert updated_ds.get('resource_id') == res_el.get('id')
            #assert updated_ds.get('total') == 5000
            field_exists = False
            for field in updated_ds.get('fields'):
                if field.get('id') == d.get('column_name'):
                    field_exists = True
                    break
            assert field_exists
        res_first = helpers.call_action('datastore_search', context=context, id=res_el.get('id'), offset=0)
        assert len(res_first.get('records')) == 100
        assert res_first.get('records')[3].get('name') == 'lalala'
        pprint.pprint(res_first)

        res_last = helpers.call_action('datastore_search', context=context, id=res_el.get('id'), offset=4900)
        assert len(res_last.get('records')) == 100
        assert res_first.get('records')[99].get('name') == 'lalala'
        pprint.pprint(res_last)

        res_none = helpers.call_action('datastore_search', context=context, id=res_el.get('id'), offset=5000)
        pprint.pprint(res_none)
        assert len(res_none.get('records')) == 0

    # TODO: Create tests in transcription, automatic translation modes

    ###
    ### Translate Resource Delete Tests
    ###
    @nose.tools.istest
    def test_c1_delete_translation_resource_invalid(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        # Should delete translation resource correctly
        trans_data_wrong = [
                {'resource_id': resource.get('id')},
                {'resource_id': 'wrong-resource-id'}]

        for d in trans_data_wrong:
            nose.tools.assert_raises(p.toolkit.ValidationError, helpers.call_action, 'translate_resource_delete', context=context, **d)

    @nose.tools.istest
    def test_c2_delete_translation_resource_column_invalid(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        resource_es_id = json.loads(resource.get('has_translations')).get('es')
        # Should delete translation resource correctly
        trans_data = {
                'resource_id': resource_es_id,
                'column_name': 'invalid-column-name'
                }

        nose.tools.assert_raises(p.toolkit.ValidationError, helpers.call_action, 'translate_resource_delete', context=context, **trans_data)

    # TODO: Create this after translate_resource_update has created some columns
    @nose.tools.istest
    def test_c3_delete_translation_resource_column(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        resource_el_id = json.loads(resource.get('has_translations')).get('el')
        resource_el = p.toolkit.get_action('datastore_search')(context, {'id':resource_el_id})
        #print resource_el
        # Should delete translation resource correctly
        trans_data = {
                'resource_id': resource_el_id,
                'column_name': 'name'
                }
        pprint.pprint(helpers.call_action('translate_resource_delete', context=context, **trans_data))
        print "RESULT"
        pprint.pprint(helpers.call_action('datastore_search', context=context, resource_id=resource_el_id))
        asd
        #nose.tools.assert_raises(p.toolkit.ValidationError, helpers.call_action, 'translate_resource_delete', context=context, **trans_data)

    @nose.tools.nottest
    def test_c4_delete_translation_resource(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        resource_es_id = json.loads(resource.get('has_translations')).get('es')
        # Should delete translation resource correctly
        trans_data = {'resource_id': resource_es_id}

        helpers.call_action('translate_resource_delete', context=context, **trans_data)
        res = helpers.call_action('resource_show', context=context, id=resource_es_id)
        assert res.get('id')
        assert res.get('state') == 'deleted'
        orig_resource = helpers.call_action('resource_show', context=context, id=resource.get('id'))
        assert 'es' not in json.loads(orig_resource.get('has_translations'))

    # TODO: Move test after basic delete tests
    @nose.tools.nottest
    def test_c5_delete_and_recreate_translation_resource(self):
        resource = self._get_initial_datastore(package_data.get('name'))
        context = self.get_context()

        resource_el_id = json.loads(resource.get('has_translations')).get('el')
        # Should delete translation resource correctly
        trans_data = {'resource_id': resource_el_id }

        helpers.call_action('translate_resource_delete', context=context, **trans_data)
        # TODO: assert oringal metadata updates - has_translations and translation resource_deleted
        #deleted_resource = helpers.assert_raises('datastore_search', context=context, resource_id= resource_el_id)
        nose.tools.assert_raises(p.toolkit.ObjectNotFound, helpers.call_action, 'datastore_search', context=context, resource_id=resource_el_id)
        #print deleted_resource

        deleted_resource = helpers.call_action('resource_show', context=context, id=resource_el_id)
        assert deleted_resource.get('state') == 'deleted'

        original_updated_resource = helpers.call_action('resource_show', context=context, id=resource.get('id'))
        assert original_updated_resource.get('has_translations')
        assert 'el' not in json.loads(original_updated_resource.get('has_translations'))

        # Try to recreate translation in same language after it has been deleted
        trans_data = {
                'package_id': package_data.get('name'),
                'resource_id': resource.get('id'),
                'language': 'el',
                }

        created_res = helpers.call_action('translate_resource_create', context=context, **trans_data)
        assert created_res.get('id')
        assert created_res.get('translation_resource')

        original_resource = helpers.call_action('resource_show', context=context, id=resource.get('id'))
        assert original_resource.get('id') == created_res.get('translation_parent_id')
        assert original_resource.get('has_translations')
        assert 'el' in json.loads(original_resource.get('has_translations'))
        assert json.loads(original_resource.get('has_translations')).get('el') == created_res.get('id')

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

