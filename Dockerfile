#big panic time.
#docker build . -t redis_to_db
FROM golang:1.16.7-alpine3.14
WORKDIR /redis_to_db
COPY . . 
RUN go build -o redis_to_db

FROM alpine:3.14
WORKDIR /redis_to_db
COPY start_redis_to_db.sh /redis_to_db/
COPY --from=0 /redis_to_db/redis_to_db /redis_to_db/
CMD chmod u+x /redis_to_db/start_redis_to_db.sh
CMD /redis_to_db/start_redis_to_db.sh