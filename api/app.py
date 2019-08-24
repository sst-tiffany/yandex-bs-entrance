import logging.config
from flask import Flask, g
import psycopg2

from api.imports.resource import endpoint as imports
from api.imports.dbm import DBManager

import settings

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('api')


app = Flask(__name__)


def connect_to_db(uri):
    return psycopg2.connect(uri)


@app.before_request
def create_conn():
    if 'conn' not in g:
        logger.info(f'Getting connection')
        g.conn = connect_to_db(settings.DB_URI)


@app.teardown_appcontext
def teardown_conn(exc):
    conn = g.pop('conn', None)
    if conn is not None:
        logger.info(f'Closing connection')
        conn.commit()
        conn.close()


app.register_blueprint(imports)
dbm = DBManager()
with app.app_context():
    g.conn = connect_to_db(settings.DB_URI)
    dbm.init_database()

application = app
