# KAFKA Consumer in Go
KAFKA consumer in Golang using Sarama

## Step 1
Add the ENVs:
`KAFKA_URL`, `KAFKA_TRUSTED_CERT`, `KAFKA_CLIENT_CERT_KEY`, `KAFKA_CLIENT_CERT`, `KAFKA_PREFIX`, `KAFKA_CONSUMER_GROUP`
in the .env file


Please note that `KAFKA_TRUSTED_CERT`, `KAFKA_CLIENT_CERT_KEY`, and `KAFKA_CLIENT_CERT` has to be **base64 encoded** values of the actual values (This is because the package `joeshaw/envdecode` doesn't support multiline envs) - This is only if you are going to use the .env file or trying this out locally.

## Step 2

Set the Kafka topic in
`kafka/kafka.go:212`

It is now set to `order_events`

## Step 3

Run the app:
```
go run main.go
```

Ensure that the consumer is running and is receiving messages.


## Step 4

Send 400 messages at once (not by looping through, but as a bulk) to a particular partition from any kafka client. The payload I tried was a JSON of ~1700 characters.

In my case, I am not receiving any messages in the consumer end :/ 
