import logging
from psycopg2.extras import execute_values, RealDictCursor
from flask import g


logger = logging.getLogger(__name__)


class DBManager:
    _schema = 'imports'

    def __init__(self, connection=None):
        self._connection = connection if connection else None

    @property
    def connection(self):
        self._connection = g.conn
        return self._connection

    # schemas, tables etc initialization
    def init_database(self):
        self._init_schema_imports()
        self._init_import_id_sequence()
        self._init_table_citizen()
        self._init_table_relation()

    def _init_schema_imports(self):
        query = f"""
            create schema if not exists {self._schema}
        """
        with self.connection.cursor() as cur:
            cur.execute(query)
        # print('schema created')

    def _init_import_id_sequence(self):
        query = f"""
            create sequence if not exists {self._schema}.import_id
        """
        with self.connection.cursor() as cur:
            cur.execute(query)
        # print('sequence created')

    def _init_table_citizen(self):
        query = f"""
            create table if not exists {self._schema}.citizen (
                import_id integer,
                citizen_id integer, 
                town varchar, 
                street varchar, 
                building varchar,
                apartment integer, 
                name varchar, 
                birth_date date, 
                gender varchar,

                primary key (import_id, citizen_id)
            )

        """
        with self.connection.cursor() as cur:
            cur.execute(query)
        # print('table citizen created')

    def _init_table_relation(self):
        query = f"""
            create table if not exists {self._schema}.relation (
                import_id integer,
                citizen_id integer, 
                relative integer, 
                is_active boolean,

                primary key (import_id, citizen_id, relative)
            )

        """
        with self.connection.cursor() as cur:
            cur.execute(query)
        # print('table relation created')

    # endpoint1: post citizens
    def insert_citizens(self, citizens):
        import_id = self._get_next_import_id()
        citizen_values, relation_values = self._insert_citizens_data_transform(citizens, import_id)

        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            logger.info(f'inserting citizens import_id={import_id}')
            query = f"""
                insert into {self._schema}.citizen (
                    import_id,
                    citizen_id, 
                    town, 
                    street, 
                    building,
                    apartment, 
                    name, 
                    birth_date, 
                    gender
                ) 
                values %s
            """
            template = '''(
                    %(import_id)s,
                    %(citizen_id)s, 
                    %(town)s, 
                    %(street)s, 
                    %(building)s,
                    %(apartment)s, 
                    %(name)s, 
                    to_date(%(birth_date)s, 'DD.MM.YYYY'), 
                    %(gender)s
                )'''
            execute_values(cur, query, citizen_values, template=template)

            logger.info(f'inserting relations import_id={import_id}')
            query = f"""
                insert into {self._schema}.relation (
                    import_id,
                    citizen_id, 
                    relative, 
                    is_active
                ) 
                values %s
            """
            template = '''(
                    %(import_id)s,
                    %(citizen_id)s, 
                    %(relative)s, 
                    %(is_active)s
                )'''
            execute_values(cur, query, relation_values, template=template)

        logger.info(f'import document inserted import_id={import_id}')
        return {'import_id': import_id}

    def _get_next_import_id(self):
        # fetchone
        query = f'select nextval(\'{self._schema}.import_id\') as import_id'
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            import_id = cur.fetchone()['import_id']
        return import_id

    def _insert_citizens_data_transform(self, citizens, import_id):
        citizen_values = [
            dict(
                **{
                    key: val for key, val in citizen.items() if key != 'relatives'
                },
                **{'import_id': import_id}
            )
            for citizen in citizens
        ]
        relation_values = self._insert_citizens_data_transform_relation(citizens, import_id)
        return citizen_values, relation_values

    def _insert_citizens_data_transform_relation(self, citizens, import_id):
        relations_data = [
            [
                {
                    'import_id': import_id,
                    'citizen_id': citizen['citizen_id'],
                    'relative': relative,
                    'is_active': True

                } for relative in citizen['relatives']

            ] for citizen in citizens
        ]
        flatten = lambda lst: [item for sublist in lst for item in sublist]
        return flatten(relations_data)

    def update_citizen(self, citizen_info):
        with self.connection.cursor() as cur:
            query = f"""
                update {self._schema}.citizen
                    set 
                        town = %(town)s,
                        street = %(street)s,
                        building = %(building)s,
                        apartment = %(apartment)s,
                        name = %(name)s,
                        birth_date = to_date(%(birth_date)s, 'DD.MM.YYYY'),
                        gender = %(gender)s
                    where
                        import_id = %(import_id)s
                        and citizen_id = %(citizen_id)s
            """  # TODO: why insert ... on conflict do update?
            cur.execute(query, citizen_info)

    def update_relation(self, relation):
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            query = f"""
                insert into {self._schema}.relation (
                    import_id,
                    citizen_id,
                    relative,
                    is_active
                ) 
                values (
                    %(import_id)s,
                    %(citizen_id)s, 
                    %(relative)s, 
                    %(is_active)s
                )
                on conflict (import_id, citizen_id, relative)
                do update
                    set 
                        import_id = %(import_id)s,
                        citizen_id = %(citizen_id)s,
                        relative = %(relative)s,
                        is_active = %(is_active)s
            """
            cur.execute(query, relation)

    # endpoint 3: get citizens
    def get_citizens(self, import_id):
        query = f"""
            with c as (
                select * 
                from {self._schema}.citizen
                where import_id = {import_id}
            ),
            r as (
                select citizen_id, array_agg(relative) as relatives
                from {self._schema}.relation
                where import_id = {import_id} and is_active
                group by citizen_id 
            ),
            c_full as (
                select 
                    c.*,
                    r.relatives
                from c
                left join r using(citizen_id)
            )
            select 
                citizen_id, 
                town, 
                street, 
                building,
                apartment, 
                name, 
                to_char(birth_date, 'DD.MM.YYYY') as birth_date, 
                gender, 
                case
                    when relatives is null then array[]::integer[]
                    else relatives
                end
            from c_full 
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            res = cur.fetchall()
        return res

    def get_citizen(self, import_id, citizen_id):
        # fetchone
        query = f"""
            with c as (
                select * 
                from {self._schema}.citizen
                where import_id = {import_id} and citizen_id = {citizen_id}
            ),
            r as (
                select citizen_id, array_agg(relative) as relatives
                from {self._schema}.relation
                where import_id = {import_id} and citizen_id = {citizen_id} and is_active
                group by citizen_id
            ),
            c_full as (
                select 
                    c.*,
                    r.relatives
                from c
                left join r using(citizen_id)
            )
            select 
                citizen_id, 
                town, 
                street, 
                building,
                apartment, 
                name, 
                to_char(birth_date, 'DD.MM.YYYY') as birth_date, 
                gender, 
                case
                    when relatives is null then array[]::integer[]
                    else relatives
                end
            from c_full
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            res = cur.fetchone()
        return res

    def get_citizen_ids(self, import_id):
        # fetchone
        query = f"""
            select array_agg(distinct(citizen_id)) as citizen_ids
            from {self._schema}.citizen
            where import_id = {import_id}
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            res = cur.fetchone()
        return res

    # endpoint 4: get birthdays
    def get_birthdays(self, import_id):
        # fetchone
        query = f"""
            with bday as (
                select citizen_id as relative, birth_date
                from {self._schema}.citizen
                where import_id = {import_id}
            ),
            rel as (
                select citizen_id, relative
                from {self._schema}.relation
                where import_id = {import_id} and is_active
            ),
            bdays as (
                select 
                    r.citizen_id, 
                    r.relative, 
                    extract(month from b.birth_date) as month
                from rel as r
                inner join bday b using(relative)
            ),
            bdays_by_citizen as (
                select 
                    b.month, 
                    b.citizen_id, 
                    count(b.relative) as presents
                from bdays as b
                group by b.month, b.citizen_id
            ),
            bdays_json as (
                select 
                    month, 
                    json_build_object('citizen_id', citizen_id, 'presents', presents) AS data
                from bdays_by_citizen
            ),
            bdays_grouped as (
                select 
                    month::integer, 
                    array_agg(data) as data
                from bdays_json
                group by month
            ),
            all_months as (
                select generate_series(1, 12) as month
            ),
            all_months_stat as (
                select 
                    m.month,
                    b.data
                from all_months m
                left join bdays_grouped b using(month)
            ),
            all_months_full_stat as (
                select 
                    month,
                    case 
                        when data is null then array[]::json[]
                        else data
                    end
                from all_months_stat
            )
            select json_object_agg(month, data) as data
            from all_months_full_stat
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            res = cur.fetchone()
        return res['data'] # TODO: query returning body of data field

    # endpoint 5: get age
    def get_age(self, import_id):
        query = f"""  
            with age as (
                select citizen_id, town, extract(year from age(birth_date)) as age 
                from {self._schema}.citizen
                where import_id = {import_id}
            )
            select 
                town, 
                percentile_cont(0.5) within group (order by age) as p50,
                percentile_cont(0.75) within group (order by age) as p75,
                percentile_cont(0.99) within group (order by age) as p99
            from age
            group by town
        """
        #     select
        #         town,
        #         round(p50::numeric, 2) as p50,
        #         round(p75::numeric, 2) as p75,
        #         round(p99::numeric, 2) as p99
        #     from age_percentile
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            res = cur.fetchall()
        return res

    def get_import_ids(self):
        # fetchone
        query = f"""
            select array_agg(distinct(import_id)) as import_ids
            from {self._schema}.citizen
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            res = cur.fetchone()
        return res['import_ids'] or []
