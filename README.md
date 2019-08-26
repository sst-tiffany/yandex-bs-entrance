# yandex-bs-entrance

## Build

Before building we need update package manager:

```bash
sudo apt update
```

### Python environment

Install required packages:

```bash
sudo apt install python3 python3-pip python3-virtualenv libpq-dev supervisor
```

### PostgreSQL
Then, install the Postgres package:
```bash
sudo apt install postgresql
```
Start Postgres server:
```bash
service postgresql start
```
Switch over to the postgres account on your server by typing:
```bash
sudo -i -u postgres
```
Sign in as `postgres` user:
```bash
psql
```
Create database with name `market`, user `dbm` with password
and grant all privileges on `market` database to `dbm`. 
Fix password and privileges in accordance to security requirements.:

```bash
CREATE DATABASE market;
CREATE USER dbm WITH ENCRYPTED PASSWORD '******';
GRANT ALL PRIVILEGES ON DATABASE market TO dbm;
```

Repeat last commands to create test database with test user.
Leave this as is (recommended):

```bash
CREATE DATABASE market_test;
CREATE USER dbm_test WITH ENCRYPTED PASSWORD 'password_test';
GRANT ALL PRIVILEGES ON DATABASE market_test TO dbm_test;
```

Create `.env` file with postgres URI. Use password created 
before for `dbm` user. This file is able to contain other 
environmental variables declared in `settings.py`:
```
# .env
DB_URI="postgres://dbm:******@localhost:5432/market"
```

## Installation
Create virtual environment with name `venv` and activate this:
```bash
python3 -m venv venv && \
source venv/bin/activate
```
Install all python packages from `requirements.txt`:
```bash
python3 -m pip install -r requirements.txt
```

## Tests
Using virtual environment install `requirements.test.txt`:
```bash
python3 -m pip install -r requirements.test.txt
```
Run tests:
```bash
pytest
```

## Run API
This application uses `Supervisor:uWSGI:Flask` stack. 
`.ini` file already created. Fix `app.ini` in accordance to 
server preferences.

Ensure that supervisor is running after installation:
```bash
service supervisor restart
```
Create `yandex-bs-entrance.conf` file in `/etc/supervisor/conf.d`:
```
[program:yandex-bs-entrance]
user = entrant
directory = /home/entrant/yandex-bs-entrance
command=uwsgi --ini app.ini
autostart=true
autorestart=true
stderr_logfile = /home/entrant/yandex-bs-entrance/err.log
stdout_logfile = /home/entrant/yandex-bs-entrance/out.log
stopsignal=INT
```
Sign in `supervisorctl`:
```bash
supervisorctl
```
Reread and update supervisor:
```bash
reread
update
```
Start the service:
```bash
start yandex-bs-entrance
```
API is running on `0.0.0.0:8080`.