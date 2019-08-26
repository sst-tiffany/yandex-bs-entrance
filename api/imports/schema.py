from datetime import datetime

from marshmallow import Schema, fields, validate, validates_schema, ValidationError, post_load, pre_load

GENDER = frozenset(['male', 'female'])


class TestSchema(Schema):
    test = fields.Str(required=True)


_not_blank = validate.Length(min=1, error='Field cannot be blank')
_not_empty_list = _not_blank


def validate_citizen_id(n):
    if n < 0:
        raise ValidationError("citizen_id must be greater than 0.")

def validate_apartment(n):
    if n < 0:
        raise ValidationError("apartment must be greater than 1.")


def _gender(gender):
    if gender not in GENDER:
        raise ValidationError(f'Unexpected gender {gender}')


def _validate_date(date):
    try:
        date = datetime.strptime(date, '%d.%m.%Y').date()
    except ValueError:
        raise ValidationError(f'Bad date format {date}')

    if date > datetime.now().date():
        raise ValidationError(f'Future date')

def _not_repeat(alist):
    if len(set(alist)) < len(alist):
        raise ValidationError('Repeat relative')


class _UnknownRaiseMixin:
    @validates_schema(pass_original=True)
    def _check_unknown(self, data, origin_data):
        if not isinstance(origin_data, dict):
            return

        loadable_fields = [k for k, v in self.fields.items() if not v.dump_only]
        unknown_fields = {key for key, value in origin_data.items()
                          if key not in loadable_fields}
        if unknown_fields:
            raise ValidationError([('Unknown field name {field}.').format(field=field)
                                   for field in unknown_fields])


class _EmptyPayloadMixin:
    @pre_load
    def _empty(self, data):
        if not data:
            raise ValidationError('Empty payload')


class CitizenBase(Schema, _UnknownRaiseMixin):
    @pre_load
    def _relatives2str(self, data):
        if 'relatives' not in data:
            return
        if not isinstance(data['relatives'], list):
            return

        data['relatives'] = [str(i) for i in data['relatives']]


class CitizenPatch(CitizenBase, _EmptyPayloadMixin):
    """
    [{
    'citizen_id': 1,
    'town': 'Москва',
    'street': 'Льва Толстого',
    'building': '16к7стр5',
    'apartment': 7,
    'name': 'Иванов Иван Иванович',
    'birth_date': '26.12.1986',
    'gender': 'male',
    'relatives': [2]
  },]
    """
    town = fields.String(validate=_not_blank)
    street = fields.String(validate=_not_blank)
    building = fields.String(validate=_not_blank)
    apartment = fields.Integer(validate=validate_apartment)
    name = fields.String(validate=_not_blank)
    gender = fields.String(validate=_gender)
    relatives = fields.List(fields.Integer(validate=validate_citizen_id), validate=_not_repeat)
    birth_date = fields.String(validate=_validate_date)


class CitizenPost(CitizenBase):
    citizen_id = fields.Integer(validate=validate_citizen_id, required=True)
    town = fields.String(validate=_not_blank, required=True)
    street = fields.String(validate=_not_blank, required=True)
    building = fields.String(validate=_not_blank, required=True)
    apartment = fields.Integer(validate=validate_apartment, required=True)
    name = fields.String(validate=_not_blank, required=True)
    gender = fields.String(validate=_gender, required=True)
    relatives = fields.List(fields.Integer(validate=validate_citizen_id), validate=_not_repeat, required=True)
    birth_date = fields.String(validate=_validate_date, required=True)


class Import(Schema, _UnknownRaiseMixin, _EmptyPayloadMixin):
    citizens = fields.List(fields.Nested(CitizenPost), validate=_not_empty_list)
