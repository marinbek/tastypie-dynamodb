from tastypie.fields import ApiField, ToOneField as TastyOneField, NOT_PROVIDED
from django.core.urlresolvers import NoReverseMatch, get_script_prefix, resolve, Resolver404
from django.utils import importlib

class PrimaryKeyField(ApiField):
    def hydrate(self, bundle):
        if bundle.request.method == 'PUT':
            return None

        return super(PrimaryKeyField, self).hydrate(bundle)


class ToOneDjangoField(TastyOneField):

    def __init__(self, to, model, model_field, dynamo_field, related_name=None, default=NOT_PROVIDED,
                 null=False, blank=False, readonly=False, full=False,
                 unique=False, help_text=None, use_in='all', full_list=True, full_detail=True,
                 separator=None, value_index=0):

        attribute = model_field
        self.separator = separator
        self.value_index = value_index
        self.model = model
        self.dynamo_field = dynamo_field
        self.model_field = model_field

        module_bits = model.split('.')
        module_path, class_name = '.'.join(module_bits[:-1]), module_bits[-1]
        module = importlib.import_module(module_path)
        self.model_class = getattr(module, class_name, None)

        super(ToOneDjangoField, self).__init__(
            to, attribute, related_name=related_name, default=default,
            null=null, blank=blank, readonly=readonly, full=full,
            unique=unique, help_text=help_text, use_in=use_in,
            full_list=full_list, full_detail=full_detail
        )

    def dehydrate(self, bundle, for_list=True):
        value = getattr(bundle.obj, self.dynamo_field)
        if not value:
            return None

        if self.separator:
            value = value.split(self.separator)[self.value_index]

        try:
            exec("obj = self.model_class.objects.get(%s='%s')" % (self.model_field, value))
        except self.model_class.DoesNotExist:
            return None

        resource = self.get_related_resource(bundle.obj)
        bundle2 = resource.build_bundle(obj)
        kwargs = resource.resource_uri_kwargs(bundle2)

        url_name = 'api_dispatch_detail'

        try:
            return resource._build_reverse_url(url_name, kwargs=kwargs)
        except NoReverseMatch:
            return ''


"""
    separator - if your hashkey is something like "SOMETHING:OTHER", ":" is separator and your
                connected resource has a hash SOMETHING, you can tell this class to separate the
                value using the separator and use only part of that
    hashkey_index - after separating key value, which part to use when connecting to a resource
"""
class ToOneField(TastyOneField):

    def __init__(self, to, attribute, related_name=None, default=NOT_PROVIDED,
                 null=False, blank=False, readonly=False, full=False,
                 unique=False, help_text=None, use_in='all', full_list=True, full_detail=True,
                 separator=None, hashkey_index=0, rangekey_index=1, aliases=None):

        self.separator = separator
        self.hashkey_index = hashkey_index
        self.rangekey_index = rangekey_index
        self.aliases = aliases

        super(ToOneField, self).__init__(
            to, attribute, related_name=related_name, default=default,
            null=null, blank=blank, readonly=readonly, full=full,
            unique=unique, help_text=help_text, use_in=use_in,
            full_list=full_list, full_detail=full_detail
        )

    def get_dynamo_keys(self, uri):
        # find hash and range keys
        hashkey = rangekey = None
        for name, field in self.to_class.base_fields.iteritems():
            if isinstance(field, HashKeyField):
                hashkey = field.attribute
            elif isinstance(field, RangeKeyField):
                rangekey = field.attribute

        # Get kwargs from uri
        prefix = get_script_prefix()
        chomped_uri = uri
        if prefix and uri.startswith(prefix):
            uri = uri[len(prefix)-1:]
        try:
            view, args, kwargs = resolve(uri)
        except Resolver404:
            raise NotFound("The URL provided '%s' was not a link to a valid resource." % uri)

        del kwargs['api_name']
        del kwargs['resource_name']
        # kwargs now contains hash_key and range_key (if applicable)
        kwargs['hash_key_name'] = self.aliases.get(hashkey, hashkey)
        kwargs['range_key_name'] = self.aliases.get(rangekey, rangekey)

        return kwargs

    def dehydrate(self, bundle, for_list=True):
        if self.aliases:
            for dest, src in self.aliases.iteritems():
                setattr(bundle.obj, dest, getattr(bundle.obj, src))

        resource = self.get_related_resource(bundle.obj)
        kwargs = resource.resource_uri_kwargs(bundle)

        url_name = 'api_dispatch_detail'

        if self.separator:
            val = getattr(bundle.obj, self.attribute).split(self.separator)
            kwargs['hash_key'] = val[self.hashkey_index]
            if resource._get_range():
                kwargs['range_key'] = val[self.rangekey_index]

        if not kwargs.get('hash_key', True) or not kwargs.get('range_key', True):
            return None

        try:
            return resource._build_reverse_url(url_name, kwargs=kwargs)
        except NoReverseMatch:
            return ''


class HashKeyField(PrimaryKeyField):
    pass

class RangeKeyField(PrimaryKeyField):
    pass

class NumberMixin(object):
    convert = lambda self, value: None if value is None else int(value)

class StringMixin(object):
    convert = lambda self, value: None if value is None else str(value)

class NumericHashKeyField(NumberMixin, HashKeyField):
    pass

class StringHashKeyField(StringMixin, HashKeyField):
    pass

class NumericRangeKeyField(NumberMixin, RangeKeyField):
    pass

class StringRangeKeyField(StringMixin, RangeKeyField):
    pass

class DynamoListField(ApiField):

    def convert(self, value):
        if value is None:
            return None

        return [val for val in value]
