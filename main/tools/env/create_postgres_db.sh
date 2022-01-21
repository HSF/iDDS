#!/bin/bash

su - postgres

psql postgres
\password
#postpass
postgres=# create database idds;
CREATE DATABASE
postgres=# create user iddsuser with encrypted password 'iddspass';
CREATE ROLE
postgres=# grant all privileges on database idds to iddsuser;
GRANT
postgres=#

psql --host=localhost --port=5432 --username=iddsuser --password --dbname=idds
