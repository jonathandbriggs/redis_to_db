#!/bin/bash
curl -i --header "Content-Type: application/json" \
  --request POST \
  --data '{"message":"pong","date":"'$(date +%s)'"}' \
  http://10.0.0.240:8081/example/table
