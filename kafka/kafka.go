package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"strings"
	"time"

	"encoding/base64"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/joeshaw/envdecode"
)

// Config : Configuration for Kafka from ENV
type Config struct {
	URL           string `env:"KAFKA_URL,required"`
	TrustedCert   string `env:"KAFKA_TRUSTED_CERT,required"`
	ClientCertKey string `env:"KAFKA_CLIENT_CERT_KEY,required"`
	ClientCert    string `env:"KAFKA_CLIENT_CERT,required"`
	Prefix        string `env:"KAFKA_PREFIX"`
	ConsumerGroup string `env:"KAFKA_CONSUMER_GROUP,default=heroku-kafka-demo-go"`
}

// Client : exported kafka
type Client struct {
	Producer sarama.AsyncProducer
	Consumer *cluster.Consumer

	config *Config
}

// Message is the raw data received by a consumer
type Message struct {
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Topic     string          `json:"topic"`
	Value     string          `json:"value"`
	Metadata  messageMetadata `json:"metadata"`
}

type messageMetadata struct {
	ReceivedAt time.Time `json:"received_at"`
}

// Connect : Connects to the Kafka brokers
func (kc *Client) Connect() *Client {
	fmt.Println("Connecting to Kafka brokers...")
	config := Config{}
	envdecode.MustDecode(&config)

	// multiline values are stored as base64 encoded strings in the .env file.
	// So parsing it :)
	if os.Getenv("ENVIRONMENT") != "production" {
		config.TrustedCert = decodeBase64(config.TrustedCert)
		config.ClientCertKey = decodeBase64(config.ClientCertKey)
		config.ClientCert = decodeBase64(config.ClientCert)
	}

	tlsConfig := config.createTLSConfig()
	brokerAddrs := config.brokerAddresses()

	// verify broker certs
	for _, b := range brokerAddrs {
		ok, err := verifyServerCert(tlsConfig, config.TrustedCert, b)
		if err != nil {
			log.Fatal("Get Server Cert Error: ", err)
		}

		if !ok {
			log.Fatalf("Broker %s has invalid certificate!", b)
		}
	}
	log.Println("All broker server certificates are valid!")

	kc.Consumer = config.createKafkaConsumer(brokerAddrs, tlsConfig)
	kc.Producer = config.createKafkaProducer(brokerAddrs, tlsConfig)
	kc.config = &config
	return kc
}

func decodeBase64(base64Data string) string {
	value, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		log.Fatal("Cannot parse base64")
	}
	return string(value)
}

// ShowNotifications : Show the rebalance notifications of consumers
func (kc *Client) ShowNotifications() {
	fmt.Println("Starting Kafka notifications go routine...")
	for {
		select {
		case notification := <-kc.Consumer.Notifications():
			if notification != nil {
				fmt.Println("Notification Type: ", notification.Type)
				fmt.Println("Notification Current: ", notification.Current)
			}
		case success := <-kc.Producer.Successes():
			if success != nil {
				fmt.Println("Successfull delivery to: ", success.Topic)
				fmt.Println("Message: ", success.Value)
			}
		}
	}
}

// ShowErrors : Show the error notifications of consumers
func (kc *Client) ShowErrors() {
	fmt.Println("Starting Kafka Errors go routine...")
	for {
		select {
		case error := <-kc.Consumer.Errors():
			if error != nil {
				fmt.Println("Error occoured: ", error)
			}
		case error := <-kc.Producer.Errors():
			if error != nil {
				fmt.Println("Error occoured: ", error)
			}
		}
	}
}

func (kc *Config) createTLSConfig() *tls.Config {
	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM([]byte(kc.TrustedCert))
	if !ok {
		log.Println("Unable to parse Root Cert:", kc.TrustedCert)
	}
	// Setup certs for Sarama
	cert, err := tls.X509KeyPair([]byte(kc.ClientCert), []byte(kc.ClientCertKey))
	if err != nil {
		log.Fatal(err)
	}

	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		InsecureSkipVerify: true,
		RootCAs:            roots,
	}

	// tlsConfig.BuildNameToCertificate()
	return tlsConfig
}

// Extract the host:port pairs from the Kafka URL(s)
func (kc *Config) brokerAddresses() []string {
	urls := strings.Split(kc.URL, ",")
	addrs := make([]string, len(urls))
	for i, v := range urls {
		u, err := url.Parse(v)
		if err != nil {
			log.Fatal(err)
		}
		addrs[i] = u.Host
	}
	return addrs
}

func verifyServerCert(tc *tls.Config, caCert string, url string) (bool, error) {
	// Create connection to server
	conn, err := tls.Dial("tcp", url, tc)
	if err != nil {
		return false, err
	}

	// Pull servers cert
	serverCert := conn.ConnectionState().PeerCertificates[0]

	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM([]byte(caCert))
	if !ok {
		return false, errors.New("Unable to parse Trusted Cert")
	}

	// Verify Server Cert
	opts := x509.VerifyOptions{Roots: roots}
	if _, err := serverCert.Verify(opts); err != nil {
		log.Println("Unable to verify Server Cert")
		return false, err
	}

	return true, nil
}

// Connect a consumer. Consumers in Kafka have a "group" id, which
// denotes how consumers balance work. Each group coordinates
// which partitions to process between its nodes.
// For the demo app, there's only one group, but a production app
// could use separate groups for e.g. processing events and archiving
// raw events to S3 for longer term storage
func (kc *Config) createKafkaConsumer(brokers []string, tc *tls.Config) *cluster.Consumer {
	config := cluster.NewConfig()

	config.Net.TLS.Config = tc
	config.Net.TLS.Enable = true
	config.Group.PartitionStrategy = cluster.StrategyRoundRobin
	config.Group.Return.Notifications = true
	config.ClientID = strings.Join([]string{kc.ConsumerGroup, time.Now().Format("20200102150405")}, "-")
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	topics := []string{kc.topic("order_events")}

	log.Printf("Consuming topic %s on brokers: %s", topics, brokers)

	err := config.Validate()
	if err != nil {
		log.Fatal(err)
	}

	consumer, err := cluster.NewConsumer(brokers, kc.group(), topics, config)
	if err != nil {
		log.Fatal(err)
	}
	return consumer
}

// Create the Kafka asynchronous producer
func (kc *Config) createKafkaProducer(brokers []string, tc *tls.Config) sarama.AsyncProducer {
	config := sarama.NewConfig()

	config.Net.TLS.Config = tc
	config.Net.TLS.Enable = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll // Default is WaitForLocal
	config.Producer.Flush.Messages = 1
	config.ClientID = strings.Join([]string{kc.ConsumerGroup, time.Now().Format("20200102150405")}, "-")

	err := config.Validate()
	if err != nil {
		log.Fatal(err)
	}
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatal(err)
	}

	return producer
}

// Prepends prefix to topic if provided
func (kc *Config) topic(topicName string) string {
	topic := ""

	if kc.Prefix != "" {
		topic = strings.Join([]string{kc.Prefix, topicName}, "")
	}

	return topic
}

// Prepend prefix to consumer group if provided
func (kc *Config) group() string {
	group := kc.ConsumerGroup

	if kc.Prefix != "" {
		group = strings.Join([]string{kc.Prefix, group}, "")
	}

	return group
}
