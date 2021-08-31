#!/bin/bash

export queueName="ingestdqueue"
export diskQueuePath="./"${queueName}
export redisClientAddr="localhost"
export redisClientPort="6379"
export redisClientPass=""
export redisClientDB=1
export redisSubscribeTopic="example.table"
export targetDatabaseType="postgres"
export targetDatabaseHost="postgres"
export targetDatabasePort=5432
export targetDatabaseObject="example.table"
export targetDatabaseDatabase="postgres" #postgres, others have a database that you need to be in before you can do things.
export targetDatabaseDSN="postgres://postgres:postgres@127.0.0.1/postgres?sslmode=disable" #sslmode=disable

#Cleanup the disk Queue.
test -d ${diskQueuePath} && rm -rf ${diskQueuePath} 

#Make the disk Queue
test -d ${diskQueuePath} || mkdir ${diskQueuePath}

#run the thing
go run ./redis_to_db.go
