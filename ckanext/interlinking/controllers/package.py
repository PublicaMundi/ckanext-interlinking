from ckan.lib.base import (
    c, request, response, render, abort, redirect, BaseController)
import ckan.plugins.toolkit as toolkit
import ckan.model as model
import json
import pprint
NotFound = toolkit.ObjectNotFound
NotAuthorized = toolkit.NotAuthorized

class InterlinkingController(BaseController):
    def resource_interlink(self, resource_id, id):
        #user_dict = self._check_access()
        #self._setup_template_variables(user_dict)
        print 'EEEEEEE'
        print resource_id
        pkg_dict = self._check_pkg_access(id)
        res = self._check_res_access(resource_id)
        self._setup_template_variables(pkg_dict, res)

        return render('package/resource_interlink.html')

    def resource_datapreview(self, resource_id, id):
        '''
        Embeded page for a resource data-preview.

        Depending on the type, different previews are loaded.  This could be an
        img tag where the image is loaded directly or an iframe that embeds a
        webpage, recline or a pdf preview.
        '''

        context = {
            'model': model,
            'session': model.Session,
            'user': c.user or c.author,
            'auth_user_obj': c.userobj
        }

        try:
            c.resource = toolkit.get_action('resource_show')(context,
                                                     {'id': resource_id})
            c.package = toolkit.get_action('package_show')(context, {'id': id})

            data_dict = {'resource': c.resource, 'package': c.package}

            c.resource_json = json.dumps(c.resource)
        except NotFound:
            abort(404, _('Resource not found'))
        except NotAuthorized:
            abort(401, _('Unauthorized to read resource %s') % id)
        else:
            return render('recline_interlink.html')

    #def edit_page(self):
    def _check_pkg_access(self, name_or_id):
        context = self._get_context()
        try:
            pkg_dict = toolkit.get_action('package_show')(context, {'id':name_or_id})
            return pkg_dict
        except NotFound:
            abort(404, _('Package not found'))
        except NotAuthorized:
            abort(401, _('Not authorized to see this page'))

    def _check_res_access(self, resource_id):
        context = self._get_context()
        try:
            pkg_dict = toolkit.get_action('resource_show')(context, {'id':resource_id})
            return pkg_dict
        except NotFound:
            abort(404, _('Resource not found'))
        except NotAuthorized:
            abort(401, _('Not authorized to see this page'))

    def _get_context(self):
        context = {
            'for_view': True,
            'user': c.user or c.author,
            'auth_user_obj': c.userobj
        }
        return context

    def _setup_template_variables(self, pkg_dict, resource):
        #c.is_sysadmin = False # Fixme: why? normally should be computed
        #c.user_dict = user_dict
        #c.is_myself = user_dict['name'] == c.user
        #c.about_formatted = h.render_markdown(user_dict['about'])
        ######print pkg_dict
        #c.pkg_dict = pkg_dict
        c.package = pkg_dict
        c.resource = resource

