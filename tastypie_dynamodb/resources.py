from django.conf.urls import url
from django.http import Http404

import boto.dynamodb
from boto.dynamodb.condition import EQ, ConditionTwoArgs
from boto.dynamodb.exceptions import DynamoDBKeyNotFoundError

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
        self._hash_key_type = int if self._meta.table.schema.hash_key_type == 'N' else str


    def dispatch_detail(self, request, **k):
        """Ensure that the hash_key is received in the correct type"""
        k['hash_key'] = self._hash_key_type(k['hash_key'])
        return super(DynamoHashResource, self).dispatch_detail(request, **k)

    def resource_uri_kwargs(self, bundle):
        kwargs = { 'api_name': self._meta.api_name,
                 'resource_name': self._meta.resource_name }
        if bundle:
            kwargs['hash_key'] = str(getattr(bundle.obj, self._meta.table.schema.hash_key_name))

        return kwargs

    prepend_urls = lambda self: [url(r'^(?P<resource_name>%s)/(?P<hash_key>.+)/$' % self._meta.resource_name, self.wrap_view('dispatch_detail'), name='api_dispatch_detail')]

    def _dynamo_update_or_insert(self, bundle, primary_keys=None):
        primary_keys = primary_keys or {}

        bundle = self.full_hydrate(bundle)
        item = self._meta.table.new_item(**primary_keys)
        
        #extract our attributes from the bundle
        attrs = bundle.obj.to_dict()
        
        #loop and add the valid values
        for key, val in attrs.items():
            if val is None:
                continue
            
            item[key] = val
        

        #if there are pks, this is an update, else it's new
        item.put() if primary_keys else item.save()


        #wrap the item and store it for return
        bundle.obj = DynamoObject(item)
        
        return bundle


    def obj_update(self, bundle, request=None, **k):
        """Issues update command to dynamo, which will create if doesn't exist."""
        return self._dynamo_update_or_insert(bundle, primary_keys=k)


    def obj_create(self, bundle, request=None, **k):
        """Creates an object in Dynamo"""
        return self._dynamo_update_or_insert(bundle)


    def obj_get(self, bundle, request=None, **k):
        """Gets an object in Dynamo"""
        try:
            item = self._meta.table.get_item(consistent_read=self._meta.consistent_read, **k)
        except DynamoDBKeyNotFoundError:
            raise Http404
            
        return DynamoObject(item)


    def obj_delete(self, request=None, **k):
        """Deletes an object in Dynamo"""
    
        item = self._meta.table.new_item(**k)
        item.delete()


    def rollback(self):
        pass

    def get_count(self, attr_filter={}):
        if self._meta.table.schema.range_key_name:
            attrs = [self._meta.table.schema.range_key_name,
                     self._meta.table.schema.hash_key_name]
        else:
            attrs = [self._meta.table.schema.hash_key_name]
            
        dynamo_filter = {}
        for key, val in attr_filter.iteritems():
            dynamo_filter[key] = boto.dynamodb.condition.EQ(val)
        _items = self._meta.table.scan(scan_filter=dynamo_filter,
                                       count=True)

    def get_uri_list(self, request, attr_filter={}):
        """ Gets a list of resource URIs of all objects in this table"""
        if self._meta.table.schema.range_key_name:
            attrs = [self._meta.table.schema.range_key_name,
                     self._meta.table.schema.hash_key_name]
        else:
            attrs = [self._meta.table.schema.hash_key_name]
            
        dynamo_filter = {}
        for key, val in attr_filter.iteritems():
            dynamo_filter[key] = boto.dynamodb.condition.EQ(val)

        # TODO do a query if filter HASH_KEY available
        _items = self._meta.table.scan(scan_filter=dynamo_filter,
                                       attributes_to_get=attrs)

        def hash_uri(item):
            return '%s%s/' % (self.get_resource_uri(), item[self._meta.table.schema.hash_key_name])
        def range_uri(item):
            return '%s%s/%s/' % (self.get_resource_uri(), item[self._meta.table.schema.hash_key_name], item[self._meta.table.schema.range_key_name])

        if self._meta.table.schema.range_key_name:
            items = [range_uri(it) for it in _items]
        else:
            items = [hash_uri(it) for it in _items]

        return items

    def get_list(self, request, **kwargs):

        dynamo_filter = {}

        # Try to filter by hash_key, if provided
        hkey = self._meta.table.schema.hash_key_name
        if hkey in request.GET:
            dynamo_filter[hkey] = boto.dynamodb.condition.EQ(request.GET[hkey])

        # should we add hash_key filter to NEXT URL
        hkey_in_next = hkey in request.GET

        esk = []
        if 'offset_hash' in request.GET:
            esk.append(request.GET['offset_hash'])
            
        if self._meta.table.schema.range_key_name:
            if 'offset_hash' in request.GET and 'offset_range' in request.GET:
                esk.append(request.GET['offset_range'])

            # a 'range' table, let's try filtering
            rkey = self._meta.table.schema.range_key_name
            if rkey in request.GET:
                dynamo_filter[rkey] = boto.dynamodb.condition.EQ(request.GET[rkey])

        limit = 20 if 'limit' not in request.GET else int(request.GET['limit'])

        if hkey in request.GET:
            # do a query, we have hash key filter
            if self._meta.table.schema.range_key_name and rkey and rkey in request.GET:
                rkc = dynamo_filter[rkey]
            else:
                rkc = None
            _items = self._meta.table.query(request.GET[hkey],
                                            range_key_condition=rkc,
                                            max_results=limit,
                                            exclusive_start_key=esk)
        else:
            _items = self._meta.table.scan(scan_filter=dynamo_filter,
                                           max_results=limit,
                                           exclusive_start_key=esk)

        items = [it for it in _items]

        paginator = self._meta.paginator_class(request.GET, items, resource_uri=self.get_resource_uri(), limit=self._meta.limit, max_limit=self._meta.max_limit,
                        collection_name=self._meta.collection_name)
        to_be_serialized = paginator.page()

        bundles = []
        for item in to_be_serialized['objects']:
            obj = DynamoObject(item)
            bundle = self.build_bundle(obj=obj, request=request)
            bundles.append(self.full_dehydrate(bundle))

        # generate 'next' URI using last_evaluated_key
        if not _items.last_evaluated_key:
            next_uri = None
        else:
            next_uri = '/api/%s/%s/?offset_hash=%s' % (kwargs['api_name'], kwargs['resource_name'], _items.last_evaluated_key[0])

            # append hash_key filter to NEXT URL if necessary
            if hkey_in_next:
                next_uri += '&%s=%s' % (hkey, request.GET[hkey])

            if self._meta.table.schema.range_key_name:
                next_uri += '&offset_range=%s' % _items.last_evaluated_key[1]

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
            setattr(new_class._meta, 'range_key_condition', EQ)

        #ensure a proper delimeter
        if not hasattr(new_class._meta, 'primary_key_delimiter'):
            setattr(new_class._meta, 'primary_key_delimiter', '/')

        #invalid delimeter
        elif getattr(new_class._meta, 'primary_key_delimiter') in (';', '&', '?'):
            raise Exception('"%" is not a valid delimeter.' % getattr(new_class._meta, 'primary_key_delimiter'))

        #if the user is asking us to auto-build their primary keys
        if getattr(new_class._meta, 'build_primary_keys', False) == True:
            schema = new_class._meta.table.schema
            new_class.base_fields[schema.range_key_name] = fields.NumericRangeKeyField(attribute=schema.range_key_name) if schema.range_key_type == 'N' else fields.StringRangeKeyField(attribute=schema.range_key_name)

        return new_class



class DynamoHashRangeResource(DynamoHashResource):
    """Resource to use for Dynamo tables that have hash and range keys."""

    __metaclass__ = DynamoRangeDeclarativeMetaclass

    def __init__(self, *a, **k):
        super(DynamoHashRangeResource, self).__init__(*a, **k)
        self._range_key_type = int if self._meta.table.schema.range_key_type == 'N' else str


    def dispatch_detail(self, request, **k):
        """Ensure that the range_key is received in the correct type"""

        k['range_key'] = self._range_key_type(k['range_key'])
        return super(DynamoHashRangeResource, self).dispatch_detail(request, **k)


    prepend_urls = lambda self: [url(r'^(?P<resource_name>%s)/(?P<hash_key>.+)%s(?P<range_key>.+)/$' % (self._meta.resource_name, self._meta.primary_key_delimiter), self.wrap_view('dispatch_detail'), name='api_dispatch_detail')]


    def resource_uri_kwargs(self, bundle):
        kwargs = { 'api_name': self._meta.api_name,
                 'resource_name': self._meta.resource_name }
        if bundle:
            kwargs['hash_key'] = str(getattr(bundle.obj, self._meta.table.schema.hash_key_name))
            kwargs['range_key'] = str(getattr(bundle.obj, self._meta.table.schema.range_key_name))


        return kwargs


    def obj_get_list11(self, request=None, **k):
        schema = self._meta.table.schema
    
        #work out the hash key
        hash_key = request.GET.get(schema.hash_key_name, None)
        
        if not hash_key:
            raise Http404
    
        #get initial params
        params = {
            'hash_key': self._hash_key_type(hash_key),
            'request_limit': self._meta.limit,
            'consistent_read': self._meta.consistent_read,
            'scan_index_forward': self._meta.scan_index_forward,
        }
        
        
        #see if there is a range key in the get request (which will override the default, if there was any)
        range_key = request.GET.get(schema.range_key_name, None)
        
        #if a range key value was specified, prepare
        if range_key:
            #get the range key condition
            range_key_condition = self._meta.range_key_condition

            #this is an instance, with default values we need to override.  convert back to class for re-instantiation.
            if not inspect.isclass(range_key_condition):
                range_key_condition = range_key_condition.__class__
        
            
            range_values = {}
            
            #this class should be instantiated with two values..
            if issubclass(range_key_condition, ConditionTwoArgs):
                range_values['v1'], range_values['v2'] = [self._range_key_type(i) for i in range_key.split(self._meta.primary_key_delimiter)]
            else:
                #setup the value that the class will be instantiated with
                range_values['v1'] = self._range_key_type(range_key)
            
            #instantiate the range condition class
            range_key_condition = range_key_condition(**range_values)
            
            #drop in the condition
            params['range_key_condition'] = range_key_condition


        #perform the query
        results = self._meta.table.query(**params)

        #return the results
        return [DynamoObject(obj) for obj in results]
