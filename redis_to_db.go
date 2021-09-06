//Redis Subscriber to file queue to Database insert.
//Todo: figure out why bombs with 100 concurrent queued
//Maybe peek regular instead of peekblock?
//check out hang diagnostics something on por 6060
//
package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis"        //Redis Client
	_ "github.com/go-sql-driver/mysql" //mysql driver
	"github.com/joncrlsn/dque"         //File Queue thing
	_ "github.com/lib/pq"              //postgres driver
)

//Constants
//const queueName = "ingestdqueue"
//const diskQueuePath = "./" + queueName
//const redisClientAddr = "localhost"
//const redisClientPort = "6379"
//const redisClientPass = ""
//const redisClientDB = 1
//const redisSubcribeTopic = "example.table"
//const targetDatabaseType = "postgres"
//const targetDatabaseHost = "postgres"
//const targetDatabasePort = 5432
//const targetDatabaseObject = "tbl"
//const targetDatabaseDatabase = "example" //postgres, others have a database that you need to be in before you can do things.

//func loadParameterFile {
//	//Parameters we need
//	//sourceRedisTopic
//	//sourceRedisConnectionString
//	//targetDatabaseObject
//	//targetDatabaseConnectionString
//}

//channel name comes in as a parmeter, ingestd publishes to schema.table as the channel name.
//target db(mysql|pg|oracle|sqlserver).schema.table is parameterized and repeated for each target.
//(current ingest defines insert to redis topic)
//(new program) defines read from redis topic -> queue -> database (parameteratized)
type queueThisData struct {
	ingestData string
}

type configData struct {
	//A Happy Little Comment.
}

func (*configData) getQueueName() string {
	return os.Getenv("queueName")
}
func (*configData) getQueuePath() string {
	return os.Getenv("diskQueuePath")
}
func (*configData) getRedisClientAddr() string {
	return os.Getenv("redisClientAddr")
}
func (*configData) getRedisClientPort() string {
	return os.Getenv("redisClientPort")
}
func (*configData) getRedisClientPass() string {
	return os.Getenv("redisClientPass")
}
func (*configData) getRedisClientDB() int {
	n, err := strconv.Atoi(os.Getenv("redisClientDB"))
	if err != nil {
		panic("redisClientDB not convertable to Int.")
	}
	return n
}
func (*configData) getRedisSubscribeTopic() string {
	return os.Getenv("redisSubscribeTopic")
}
func (*configData) getTargetDatabaseType() string {
	return os.Getenv("targetDatabaseType")
}
func (*configData) getTargetDatabaseObject() string {
	return os.Getenv("targetDatabaseObject")
}
func (*configData) getTargetDatabaseDatabase() string {
	//May not be needed. Pending testing and usablity of not postgres
	return os.Getenv("targetDatabaseDatabase")
}
func (*configData) getTargetDatabaseDSN() string {
	return os.Getenv("targetDatabaseDSN")
}

// Builder - abstracts out a dbRecord for dque to work
// Something something memory pointer something.
func queueThisDataBuilder() interface{} {
	return &queueThisData{}
}

//Lifted from gps_collector.go
func makeQueue(c configData) *dque.DQue {
	//Pull in Relevant env.vars.
	qName := c.getQueueName()
	qDir := c.getQueuePath()
	//
	log.Println("qDir=", qDir)
	segmentSize := 1000
	q, err := dque.NewOrOpen(qName, qDir, segmentSize, queueThisDataBuilder)
	if err != nil {
		panic(err)
	}
	return q
}

//FunctionWhich Subs to Redis and Drops to Queue
func fncSubbedToRedis(c configData, q *dque.DQue) {

	//log.Println("Redis Addr: " + c.getRedisClientAddr())
	//log.Println("Redis port: " + c.getRedisClientPort())
	//log.Println("Redis pass: " + c.getRedisClientPass())
	//log.Println("Redis DB  : " + strconv.Itoa(c.getRedisClientDB()))
	//Create new Redis Client
	redisClient := redis.NewClient(&redis.Options{
		Addr:     c.getRedisClientAddr() + ":" + c.getRedisClientPort(),
		Password: c.getRedisClientPass(),
		DB:       c.getRedisClientDB(),
	})

	//Ping redis server and see if we had any errors.
	err := redisClient.Ping().Err()

	if err != nil {
		//Sleep 3, try again.
		time.Sleep(3 * time.Second)

		//try again
		err := redisClient.Ping().Err()
		if err != nil {
			panic(err)
		}
	}

	log.Println("Subscribing to Topic: " + c.getRedisSubscribeTopic())
	//Subscribe to that topic thing
	topic := redisClient.Subscribe(c.getRedisSubscribeTopic())

	//We're getting a channel
	//Channel is basically a built-in for pinging and pulling messages nicely.
	channel := topic.Channel()

	//For messages in channel
	for msg := range channel {
		//instantiate a copy of the struct where we're storing data.
		//Unmarshal the data into the user.
		//For Debug
		log.Println("enq:" + msg.Payload)
		//Drop the message to queue.
		q.Enqueue(msg.Payload)
		//log.Println("Queue Size", q.Size())
		engageTurbo(c, q)

		//log.Println("Sub to Redis side: Queue Size", q.Size())
	}
}

func fncQueuetoDB(c configData, q *dque.DQue, db *sql.DB) {
	//Persist variables because we need to reference them like a million times.
	targetDatabaseType := c.getTargetDatabaseType()
	targetDatabaseObject := c.getTargetDatabaseObject()

	//Loop always
	for {
		//Peek Top Message and try to insert it.
		//If the insert was good remove that message.
		//repeat.
		var iface interface{}
		var err error

		//log.Println("Queue to DB Side: Queue Size: ", q.Size())
		engageTurbo(c, q)

		iface, err = q.Peek()
		switch err {
		//case dque.ErrCorruptedSegment:
		//	log.Fatal("Corrupted Segment. Purge newest File.")
		case dque.ErrQueueClosed:
			q = makeQueue(c)
		}

		if iface == nil && q.Size() == 0 {
			time.Sleep(3 * time.Second)
			log.Println("Empty Queue Nap")
			continue
		}
		if iface == nil && q.Size() != 0 {
			_, err = q.Dequeue()
			if err != nil {
				panic("Nil iface on non-zero Queue.")
			}

		}
		log.Println("peek:", iface)
		//At this iface is an interfacing containing a string. That string is json.

		//Using reflect, copy the iface value into v.
		//v is a dumb name, but we only use it for a few rows.
		//This reflect thing allows us to dynamically type.
		v := reflect.ValueOf(iface)
		//the Kind function returns what datatype v is holding.
		//If we didn't get a string here, something is wrong.
		//At this point in time, that would be mostly a program problem. not a runtime problem.
		if v.Kind() != reflect.String {
			panic("We didn't get a string from our reflect call!")
		}

		//Define the Map. This will be used to contain the json from ingestd as a map so we can build insert statement.
		var dbData map[string]interface{}

		//Unmarshal json data from queue into the Map.
		err = json.Unmarshal([]byte(v.String()), &dbData)
		if err != nil {
			log.Println("Errors with Unmarshal")
			q.DequeueBlock()
		}

		//build a list to hold keys
		keyList := make([]string, 0, len(dbData))
		//Append known keys to list.
		for k := range dbData {
			keyList = append(keyList, k)
		}
		//Sort keeeys
		sort.Strings(keyList)

		//start insert statement parts
		insertPt1 := "insert into " + targetDatabaseObject + "("
		var insertPt2 string

		//loop counter variable
		i := 1

		//Set Bind Variable Name
		var bvName string

		//To expand the capability of this with more database platform handling.
		//Static Bind Variable Naming goes here.
		switch targetDatabaseType {
		case "mysql":
			bvName = "?"
		}

		//define the params interface.
		var params []interface{}
		//Loop through dbData, splitting keys and values
		//Use the order from keyList
		//Anecdotally this seems to work.
		//Yoinked it from https://yourbasic.org/golang/sort-map-keys-values/
		for _, k := range keyList {
			//To expand capability of this program with more database platform handling
			//Dynamic Bind Variable Naming goes here.
			switch targetDatabaseType {
			case "postgres":
				bvName = "$" + strconv.Itoa(i)
			case "oracle":
				bvName = ":b" + strconv.Itoa(i)
			}
			//For the first key, append the key name and key values to the appropriate strings.
			if i == 1 {
				insertPt1 += k
				insertPt2 += bvName

			} else {
				//For all further keys, append a comma, then the name and value to appropriate strings.
				insertPt1 += "," + k
				insertPt2 += "," + bvName
			}
			//Append the value to params array, which is used during the sql execution.
			params = append(params, dbData[k])
			i++
		}

		//finalize the insert statement into a single string.
		//Technically we could just do this in the exec statement, but hey.
		insert := insertPt1 + " ) VALUES ( " + insertPt2 + " )"

		log.Println(insert, params)
		_, err = db.Exec(insert, params...)
		if err == nil {
			log.Println("deq!")
			q.DequeueBlock()
		} else {
			log.Println("Error at the DB insert part of the queue to db loop.")
			log.Println(err)
			//log.Fatal("Error at the DB Insert part of the DB Loop. Error: ", err)
		}
	}
}

func initDB(c configData) *sql.DB {
	targetDatabaseType := c.getTargetDatabaseType()
	targetDatabaseDSN := c.getTargetDatabaseDSN()
	var err error
	var db *sql.DB
	//setup target db connection.
	switch targetDatabaseType {
	case "postgres", "mysql":
		//mysql and postgres have different connection details
		//postgresconnstr := "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full"
		//mysql   connstr := "username:password@protocol(address)/dbname?param=value"
		log.Println("connecting with dbtype", targetDatabaseType, "and dsn", targetDatabaseDSN)
		db, err = sql.Open(targetDatabaseType, targetDatabaseDSN)
		if err == nil {
			log.Println("connected to database. doing a ping()")
			err = db.Ping()
			if err != nil {
				log.Println(err)
				//panic(err)
			}
			log.Println("did a ping()")
		} else {
			log.Println("error connecting to database after open", err)
			//panic("error")
		}
	default:
		panic("fncqueuetodb: not ready to target type " + targetDatabaseType)
	}

	//set connection parameterws
	db.SetConnMaxLifetime(1 * time.Hour)
	db.SetConnMaxIdleTime(15 * time.Minute)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)

	return db
}
func checkDB(c configData, db *sql.DB) {
	var err error
	for {
		err = db.Ping()
		if err != nil {
			db.Close()
			db = initDB(c)
			break
		} else {
			time.Sleep(30)
		}
	}
}

func engageTurbo(c configData, q *dque.DQue) {
	return
	if q.Size() > 10 {
		if !q.Turbo() {
			log.Print("Setting Turbo ON")
			q.TurboOn()
		}
	} else {
		if q.Turbo() {
			log.Print("Setting Turbo OFF")
			q.TurboOff()
		}
	}
}

func main() {
	//Suck up some Environment Variables

	theConfig := configData{}

	db := initDB(theConfig)

	//Start the Queue.
	q := makeQueue(theConfig)

	log.Println("oh hello")
	//Open the Redis Listener which places messages on the queue.
	go fncSubbedToRedis(theConfig, q)
	//Open the Database handler which loads the messages from the queue to your target db.
	fncQueuetoDB(theConfig, q, db)

}
