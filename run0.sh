#!/bin/bash

go generate ./...
go build -a .
#docker build --no-cache --pull -t highloadcup2018 .
#docker run --rm -it -p 8000:80 -t highloadcup2018
mkdir -p /tmp/data
cp ./test_accounts0/data/data.zip /tmp/data/
cp ./test_accounts0/data/options.txt /tmp/data/
./highloadcup2018 -addr ":8000"