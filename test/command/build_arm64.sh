#!/bin/sh



if [ -f smss-cli ]; then
    rm smss-cli
else
    echo "smss-cli does not exist"
fi

GOOS=linux GOARCH=arm64 go build -ldflags "-s -w" -o smss-cli