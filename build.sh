#!/bin/bash

export GOPATH=`pwd`

rm -rf mqtt 
go build -o mqtt src/main.go
