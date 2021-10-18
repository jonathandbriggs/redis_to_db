#!/bin/sh
##2021.Sep.05 - jonathandbriggs
##  Wrapper script to purge redis queue and start the program.
##  Long-term we should be able to open the queue, and only purge the current data IF there is a problem with opening the queue.

export queueName="redis_to_db_queue"
export diskQueuePath="./q/"${queueName}
#export redisClientAddr="redishost"
#export redisClientPort="6379"
#export redisClientPass=""
#export redisClientDB=1
#export redisSubscribeTopic="example.table"
#export targetDatabaseType="postgres"
#export targetDatabaseHost="unused, change dsn."
#export targetDatabasePort=5432
#export targetDatabaseObject="example.table"
#export targetDatabaseDatabase="postgres" #postgres, others have a database that you need to be in before you can do things.
#export targetDatabaseDSN="postgres://postgres:postgres@postgreshost/postgres?sslmode=disable" #sslmode=disable

#Cleanup the disk Queue.
test -d ${diskQueuePath} && rm -rf ${diskQueuePath} 

#Make the disk Queue
test -d ${diskQueuePath} || mkdir -p ${diskQueuePath}

#run the thing
./redis_to_db
