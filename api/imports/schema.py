from marshmallow import Schema, fields, validate, validates_schema, ValidationError

GENDER = frozenset(['male', 'female'])


class TestSchema(Schema):
    test = fields.Str(required=True)


_not_blank = validate.Length(min=1, error='Field cannot be blank')


def validate_citizen_id(n):
    if n < 0:
        raise ValidationError("citizen_id must be greater than 0.")

def validate_apartment(n):
    if n < 1:
        raise ValidationError("apartment must be greater than 1.")


def _gender(gender):
    if gender not in GENDER:
        raise ValidationError(f'Unexpected gender {gender}')


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


class Citizen(Schema, _UnknownRaiseMixin):
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

    citizen_id = fields.Integer(validate=validate_citizen_id)
    town = fields.String(validate=_not_blank)
    street = fields.String(validate=_not_blank)
    building = fields.String(validate=_not_blank)
    apartment = fields.Integer(validate=validate_apartment)
    name = fields.String(validate=_not_blank)
    gender = fields.String(validate=_gender)
    relatives = fields.List(fields.Integer(validate=validate_citizen_id))
    birth_date = fields.String()


class Import(Schema, _UnknownRaiseMixin):
    citizens = fields.List(fields.Nested(Citizen))
