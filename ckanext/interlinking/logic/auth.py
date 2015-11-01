import ckan.plugins as p


def interlinking_resource_auth(context, data_dict, privilege='resource_update'):
    if not 'id' in data_dict:
        data_dict['id'] = data_dict.get('resource_id')
    user = context.get('user')

    authorized = p.toolkit.check_access(privilege, context, data_dict)

    if not authorized:
        return {
            'success': False,
            'msg': p.toolkit._('User {0} not authorized to update resource {1}'
                    .format(str(user), data_dict['id']))
        }
    else:
        return {'success': True}


def interlinking_resource_create(context, data_dict):
    return interlinking_resource_auth(context, data_dict)


def interlinking_resource_update(context, data_dict):
    return interlinking_resource_auth(context, data_dict)


def interlinking_resource_delete(context, data_dict):
    return interlinking_resource_auth(context, data_dict, privilege='resource_delete')

def interlinking_resource_finalize(context, data_dict):
    return interlinking_resource_auth(context, data_dict)

def interlinking_check_full_interlink(context, data_dict):
    return interlinking_resource_auth(context, data_dict)

# TODO: grant auth to all publishers
def interlinking_get_reference_resources(context, data_dict):
    return interlinking_resource_auth(context, data_dict)
