#!/usr/bin/env bash

PYTHON=$1

echo "using ${PYTHON}"
${PYTHON} -m venv venv
./venv/bin/${PYTHON} -m pip install -r requirements.txt
