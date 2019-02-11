#!/bin/bash

go generate ./...
go build -a -ldflags="-w -s" .
docker build --no-cache --pull -t highloadcup2018 .
docker login stor.highloadcup.ru
docker tag highloadcup2018 stor.highloadcup.ru/accounts/maltese_builder
docker push stor.highloadcup.ru/accounts/maltese_builder
