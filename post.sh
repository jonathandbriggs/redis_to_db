#!/bin/bash
curl -i --header "Content-Type: application/json" \
  --request POST \
  --data '{"message":"pong","date":"'$(date +%s)'"}' \
  http://localhost:8080/example/table
