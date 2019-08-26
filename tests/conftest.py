import pytest

from api.app import app, connect_to_db
import settings


def truncate(conn, schema='imports'):
    query = f'TRUNCATE TABLE {schema}.%s CASCADE'
    for table in {'citizen', 'relation'}:
        with conn.cursor() as cur:
            cur.execute(query % table)


@pytest.fixture
def conn():
    conn_ = connect_to_db(settings.DB_URI)
    yield conn_
    conn_.commit()
    conn_.close()


@pytest.fixture
def client(conn):
    app.config['TESTING'] = True
    client = app.test_client()

    with app.app_context():
        pass

    yield client
    truncate(conn)

