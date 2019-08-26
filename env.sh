#!/usr/bin/env bash

apt install python3 python3-pip python3-virtualenv \
            postgresql libpq-dev
python3 -m venv venv
./venv/bin/python3 -m pip install -r requirements.txt
