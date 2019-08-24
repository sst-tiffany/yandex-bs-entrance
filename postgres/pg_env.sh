#!/usr/bin/env bash

printf "$(cat migrations/0.0.1__init.sql)" "$DBMPASS"