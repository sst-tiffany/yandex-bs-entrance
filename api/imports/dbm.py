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

    # endpoint2: patch citizen

    # endpoint 3: get citizens
    # def get_citizens(self, import_id):
    #     query = f"""
    #         with c as (
    #             select *
    #             from imports.citizen
    #             where import_id = {import_id}
    #         ),
    #         r as (
    #             select citizen_id, array_agg(relative) as relatives
    #             from imports.relation
    #             where import_id = {import_id} and is_active
    #             group by citizen_id
    #         ),
    #         c_full as (
    #             select
    #                 c.*,
    #                 r.relatives
    #             from c
    #             left join r using(citizen_id)
    #         )
    #
    #         select
    #             citizen_id,
    #             town,
    #             street,
    #             building,
    #             apartment,
    #             name,
    #             to_char(birth_date, 'DD.MM.YYYY'),
    #             gender,
    #             case
    #                 when relatives is null then array[]::integer[]
    #                 else relatives
    #             end
    #         from c_full
    #     """
    #     res = self.pg_query_exec(query)
    #     return res