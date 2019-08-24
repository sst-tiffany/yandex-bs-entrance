import pytest

from api.app import app


@pytest.fixture
def client():
    app.config['TESTING'] = True
    client = app.test_client()

    with app.app_context():
        pass

    yield client


def test_imports_simple(client):
    rv = client.post('/imports', json={'test': ''})
    assert rv.json == {'data': None}
