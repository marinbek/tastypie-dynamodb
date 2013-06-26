from tastypie.fields import ApiField, ToOneField as TastyOneField

class PrimaryKeyField(ApiField):
    def hydrate(self, bundle):
        if bundle.request.method == 'PUT':
            return None
        
        return super(DynamoKeyField, self).hydrate(bundle)


class ToOneField(TastyOneField):

    def dehydrate(self, bundle):
        resource = self.get_related_resource(bundle.obj)
        return resource.get_resource_uri(bundle)


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
