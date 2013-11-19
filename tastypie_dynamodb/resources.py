import copy
from django.conf.urls import url
from django.http import Http404

from tastypie.exceptions import NotFound
from django.core.exceptions import MultipleObjectsReturned
from tastypie import http
from tastypie.utils import dict_strip_unicode_keys
import boto.dynamodb2

from tastypie.resources import DeclarativeMetaclass, Resource
from tastypie_dynamodb.objects import DynamoObject

from tastypie_dynamodb import fields


class DynamoDeclarativeMetaclass(DeclarativeMetaclass):
    """
    Metaclass for Dynamo tables with a hash key.
    This populates some defaults on the _meta attribute and fills in a hash field if necessary.
    """

    def __new__(cls, name, bases, attrs):
        #if no hash_key is specified
        #if len([attr for attr in attrs.values() if isinstance(attr, fields.HashKeyField)]) == 0:

        new_class = super(DynamoDeclarativeMetaclass, cls).__new__(cls, name, bases, attrs)

        #ensure that consistent_read has a value
        if not hasattr(new_class._meta, 'consistent_read'):
            setattr(new_class._meta, 'consistent_read', False)

        #ensure that object_class has a value
        if getattr(new_class._meta, 'object_class', None) == None:
            setattr(new_class._meta, 'object_class', DynamoObject)

        #if the user is asking us to auto-build their primary keys
        if getattr(new_class._meta, 'build_primary_keys', False) == True:
            schema = new_class._meta.table.schema
            new_class.base_fields[schema.hash_key_name] = fields.NumericHashKeyField(attribute=schema.hash_key_name) if schema.hash_key_type == 'N' else fields.StringHashKeyField(attribute=schema.hash_key_name)

        return new_class


class DynamoHashResource(Resource):
    """Resource to use for Dynamo tables that only have a hash primary key."""

    __metaclass__ = DynamoDeclarativeMetaclass

    def __init__(self, *a, **k):
        super(DynamoHashResource, self).__init__(*a, **k)

        # There is a bug in boto, which doesn't assign proper
        # data_type to table.schema fields. We correct this here
        schema = self._meta.table.describe()
        field_defs = copy.deepcopy(schema['Table']['AttributeDefinitions'])
        self.table_schema = copy.deepcopy(self._meta.table.schema)

        for orig_field in self.table_schema:
            fdef = filter(lambda field: field['AttributeName'] == orig_field.name, field_defs)[0]
            orig_field.data_type = fdef['AttributeType']

        # Get data_Type of hash key
        self._hash_key_type = int if self._get_hash().data_type == 'N' else str

        # Get list of all indexed fields
        self._meta.indexes = {}
        for index in self._meta.table.indexes:
            # Get all indexes and then find RANGE in there
            self._meta.indexes[index.name] = filter(lambda part: part.attr_type=='RANGE', index.parts)[0].name

    def _get_hash(self):
        tmp = filter(lambda field: field.attr_type == 'HASH', self.table_schema)
        if tmp:
            return tmp[0]
        raise Exception('Couldn\'t find HashKey!')

    def _get_range(self):
        tmp = filter(lambda field: field.attr_type == 'RANGE', self.table_schema)
        return tmp[0] if tmp else None

    def dispatch_detail(self, request, **k):
        """Ensure that the hash_key is received in the correct type"""
        k['hash_key'] = self._hash_key_type(k['hash_key'])

        if self._get_range():
            if type(k['range_key']) is unicode and k['range_key'][-1] == '*':
                # List all, do a query actually instead
                return self.get_list(request, **k)
            k['range_key'] = self._range_key_type(k['range_key'])
        return super(DynamoHashResource, self).dispatch_detail(request, **k)

    def resource_uri_kwargs(self, bundle):
        kwargs = { 'api_name': self._meta.api_name,
                 'resource_name': self._meta.resource_name }
        if bundle:
            kwargs['hash_key'] = getattr(bundle.obj, self._get_hash().name)
            if kwargs['hash_key']:
                kwargs['hash_key'] = str(kwargs['hash_key'])

        return kwargs

    def prepend_urls(self):
        return [
            url(r'^(?P<resource_name>%s)/(?P<hash_key>.+)/$' % self._meta.resource_name, self.wrap_view('dispatch_detail'), name='api_dispatch_detail'),
        ]

    def get_dynamo_filter(self, kwargs):
        filt = dict()
        filt[self._get_hash().name] = kwargs['hash_key']
        if self._get_range():
            filt[self._get_range().name] = kwargs['range_key']
        return filt

    def _dynamo_update_or_insert(self, bundle, primary_keys=None, force_put=False):
        bundle = self.full_hydrate(bundle)

        if primary_keys:
            filt = self.get_dynamo_filter(primary_keys)
            # Extract primary keys
            if force_put:
                item = filt
            else:
                item = self._meta.table.get_item(**filt)
                if not item.values():
                    raise Http404()
        else:
            # An attempt to create a new item
            item = dict()

        # extract our attributes from the bundle
        attrs = bundle.obj.to_dict()

        # loop and add the valid values from the given bundle
        # to the dynamo item
        for key, val in attrs.items():
            if val is None:
                continue
            item[key] = val

        # if there are keys, this is an update, else it's new
        if not primary_keys or force_put:
            # New or PUTting item
            self._meta.table.put_item(item, overwrite=force_put)
        else:
            # Save and overwrite if item exists already
            item.save(overwrite=True)

        # wrap the item and store it for return
        bundle.obj = DynamoObject(item)

        return bundle

    def obj_update(self, bundle, request=None, **k):
        """Issues update command to dynamo, which will create if doesn't exist."""
        return self._dynamo_update_or_insert(bundle, primary_keys=k, force_put=True)

    def obj_create(self, bundle, request=None, **k):
        """Creates an object in Dynamo"""
        return self._dynamo_update_or_insert(bundle)

    def obj_get(self, bundle, request=None, **k):
        """Gets an object in Dynamo"""
        filt = self.get_dynamo_filter(k)
        item = self._meta.table.get_item(consistent=self._meta.consistent_read, **filt)
        if not item.values():
            raise Http404
        return DynamoObject(item)

    def obj_delete(self, bundle, **k):
        """Deletes an object in Dynamo"""
        filt = self.get_dynamo_filter(k)
        item = self._meta.table.get_item(consistent=self._meta.consistent_read, **filt)
        if item.values():
            item.delete()

    def patch_detail(self, request, **kwargs):
        deserialized = self.deserialize(request, request.body, format=request.META.get('CONTENT_TYPE', 'application/json'))
        deserialized = self.alter_deserialized_detail_data(request, deserialized)
        bundle = self.build_bundle(data=dict_strip_unicode_keys(deserialized), request=request)

        try:
            updated_bundle = self._dynamo_update_or_insert(bundle, primary_keys=self.remove_api_resource_names(kwargs))

            if not self._meta.always_return_data:
                return http.HttpNoContent()
            else:
                updated_bundle = self.full_dehydrate(updated_bundle)
                updated_bundle = self.alter_detail_data_to_serialize(request, updated_bundle)
                return self.create_response(request, updated_bundle)
        except (NotFound, MultipleObjectsReturned):
            updated_bundle = self.obj_create(bundle=bundle, **self.remove_api_resource_names(kwargs))
            location = self.get_resource_uri(updated_bundle)

            if not self._meta.always_return_data:
                return http.HttpCreated(location=location)
            else:
                updated_bundle = self.full_dehydrate(updated_bundle)
                updated_bundle = self.alter_detail_data_to_serialize(request, updated_bundle)
                return self.create_response(request, updated_bundle, response_class=http.HttpCreated, location=location)

    def rollback(self):
        pass

    def get_count(self, attr_filter={}):
        # if self._get_range().name):
        #     attrs = [self._get_range().name),
        #              self._get_hash().name)]
        # else:
        #     attrs = [self._get_hash().name)]

        dynamo_filter = {}
        for key, val in attr_filter.iteritems():
            dynamo_filter[key + '__eq'] = val
        _items = self._meta.table.scan(**dynamo_filter)

    def get_uri_list(self, request, attr_filter={}):
        """ Gets a list of resource URIs of all objects in this table"""
        # if self._get_range().name):
        #    attrs = [self._get_range().name,
        #             self._get_hash().name]
        # else:
        #    attrs = [self._get_hash().name]

        dynamo_filter = {}
        for key, val in attr_filter.iteritems():
            dynamo_filter[key + '__eq'] = val

        # TODO do a query if filter HASH_KEY available
        # TODO see if you can limit attributes that you get
        _items = self._meta.table.scan(**dynamo_filter)

        def hash_uri(item):
            return '%s%s/' % (self.get_resource_uri(), item[self._get_hash().name])
        def range_uri(item):
            return '%s%s/%s/' % (self.get_resource_uri(), item[self._get_hash().name], item[self._get_range().name])

        if self._get_range():
            items = [range_uri(it) for it in _items]
        else:
            items = [hash_uri(it) for it in _items]

        return items

    def get_list(self, request, **kwargs):

        dynamo_filter = {}

        # should we add hash_key filter to NEXT URL
        hkey_in_next = False
        hash_key_filter = None

        hkey = self._get_hash().name

        # Trying to filter by HASH key
        if hkey in request.GET or 'hash_key' in kwargs:
            hkey_in_next = True
            value = request.GET.get(hkey, kwargs.get('hash_key', None))
            hash_key_filter = value
            dynamo_filter[hkey + '__eq'] = value

        # Exclusive start key - when offset is required
        esk = {}
        if 'offset_hash' in request.GET:
            hash_offset = request.GET['offset_hash']
            if self._get_hash().data_type == 'N':
                hash_offset = int(hash_offset)
            esk[self._get_hash().name] = request.GET['offset_hash']

        # We are dealing with a range table!
        if self._get_range():
            # Exclusive start key
            if 'offset_hash' in request.GET and 'offset_range' in request.GET:
                range_offset = request.GET['offset_range']
                if self._get_range().data_type == 'N':
                    range_offset = int(range_offset)
                esk[self._get_range().name] = range_offset

            # Filtering by range key
            rkey = self._get_range().name
            if rkey in request.GET or 'range_key' in kwargs:
                value = request.GET.get(rkey, kwargs['range_key'])
                if value != '*':
                    if value[-1] == '*':
                        # wildcard filer, we need begins_with
                        dynamo_filter[rkey + '__beginswith'] = value[:-1]
                    else:
                        if type(value) is unicode and value.lower() in ('true', 'false'):
                            value = 0 if value.lower() == 'false' else 1
                        if self._get_range().data_type == 'N':
                            value = int(value)
                        dynamo_filter[rkey + '__eq'] = value

        limit = 20 if 'limit' not in request.GET else int(request.GET['limit'])

        if esk:
            dynamo_filter['exclusive_start_key'] = esk

        # Are we trying to filter?
        if hash_key_filter:

            # Check if trying to filter by indexed key
            selected_indexes = set(request.GET.keys()).intersection(set(self._meta.indexes.values()))

            if selected_indexes:
                for index_field in selected_indexes:
                    val = request.GET[index_field]
                    # TODO there is a bug in boto saying that index field is STRING
                    # when it is actually a NUMBER. Try to find a solution for this
                    if type(val) is unicode and val.lower() in ('true', 'false'):
                        val = 0 if val.lower() == 'false' else 1
                    dynamo_filter[index_field + '__eq'] = val
                    for index_name, val in self._meta.indexes.iteritems():
                        if val == index_field:
                            break
                    dynamo_filter['index'] = index_name

            _items = self._meta.table.query(limit=limit,
                                            **dynamo_filter)
        else:
            _items = self._meta.table.scan(limit=limit,
                                           **dynamo_filter)

        items = [it for it in _items]

        paginator = self._meta.paginator_class(request.GET, items, resource_uri=self.get_resource_uri(), limit=self._meta.limit, max_limit=self._meta.max_limit,
                        collection_name=self._meta.collection_name)
        to_be_serialized = paginator.page()

        bundles = []
        for item in to_be_serialized['objects']:
            obj = DynamoObject(item)
            bundle = self.build_bundle(obj=obj, request=request)
            bundles.append(self.full_dehydrate(bundle))

        # generate 'next' URI using _last_key_seen
        if not _items._last_key_seen:
            next_uri = None
        else:
            next_uri = '/api/%s/%s/?offset_hash=%s' % (kwargs['api_name'], kwargs['resource_name'], _items._last_key_seen[self._get_hash().name])

            # append hash_key filter to NEXT URL if necessary
            if hkey_in_next:
                next_uri += '&%s=%s' % (hkey, hash_key_filter)

            if self._get_range():
                next_uri += '&offset_range=%s' % _items._last_key_seen[self._get_range().name]

            if 'limit' in request.GET:
                next_uri += '&limit=%s' % request.GET['limit']
            if 'format' in request.GET:
                next_uri += '&format=%s' % request.GET['format']

        to_be_serialized['meta']['next'] = next_uri

        to_be_serialized[self._meta.collection_name] = bundles
        to_be_serialized = self.alter_list_data_to_serialize(request, to_be_serialized)
        return self.create_response(request, to_be_serialized)

    def obj_delete_list(self, request=None, **k):
        pass



class DynamoRangeDeclarativeMetaclass(DynamoDeclarativeMetaclass):
    """
    Metaclass for Dynamo Tables with Hash/Range Primary Keys.
    This fills in some defaults on the _meta attribute, as well as insert a range key field if necessary.
    """

    def __new__(cls, name, bases, attrs):
        new_class = super(DynamoRangeDeclarativeMetaclass, cls).__new__(cls, name, bases, attrs)

        #ensure scan index forward
        if not hasattr(new_class._meta, 'scan_index_forward'):
            setattr(new_class._meta, 'scan_index_forward', True)

        #ensure range key condition
        if not hasattr(new_class._meta, 'range_key_condition'):
            setattr(new_class._meta, 'range_key_condition', '__eq')

        #ensure a proper delimeter
        if not hasattr(new_class._meta, 'primary_key_delimiter'):
            setattr(new_class._meta, 'primary_key_delimiter', '/')

        #invalid delimeter
        elif getattr(new_class._meta, 'primary_key_delimiter') in (';', '&', '?'):
            raise Exception('"%" is not a valid delimeter.' % getattr(new_class._meta, 'primary_key_delimiter'))

        #if the user is asking us to auto-build their primary keys
        if getattr(new_class._meta, 'build_primary_keys', False) == True:
            schema = new_class._meta.table.schema
            new_class.base_fields[self._get_range().name] = fields.NumericRangeKeyField(attribute=self._get_range().name) if schema.range_key_type == 'N' else fields.StringRangeKeyField(attribute=self._get_range().name)

        return new_class



class DynamoHashRangeResource(DynamoHashResource):
    """Resource to use for Dynamo tables that have hash and range keys."""

    __metaclass__ = DynamoRangeDeclarativeMetaclass

    def __init__(self, *a, **k):
        super(DynamoHashRangeResource, self).__init__(*a, **k)
        self._range_key_type = int if self._get_range().data_type == 'N' else str

    def prepend_urls(self):
        return [
            url(r'^(?P<resource_name>%s)/(?P<hash_key>.+)%s(?P<range_key>.+)/$' % (self._meta.resource_name, self._meta.primary_key_delimiter), self.wrap_view('dispatch_detail'), name='api_dispatch_detail'),
        ]

    def resource_uri_kwargs(self, bundle):
        kwargs = { 'api_name': self._meta.api_name,
                 'resource_name': self._meta.resource_name }
        if bundle:
            kwargs['hash_key'] = getattr(bundle.obj, self._get_hash().name)
            kwargs['range_key'] = getattr(bundle.obj, self._get_range().name)
            for key in ('hash_key', 'range_key'):
                if kwargs[key]:
                    kwargs[key] = str(kwargs[key])

        return kwargs
