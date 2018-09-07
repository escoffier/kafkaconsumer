package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

var (
	addr  = []string{"192.168.21.248:9092"}
	topic = "pageviews"
)

func main() {
	//clusterConsumer()
	//clusterPartConsumer()
	consumer()

}

func consumer() {
	log.Println("NewConsumer")
	consumer, err := sarama.NewConsumer(addr, nil)

	if err != nil {
		log.Println("NewConsumer err: ", err)
		panic(err)
	}

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()
	// ts, _ := consumer.Topics()
	// log.Println("Topics: ", ts)

	// for _, t := range ts {
	// 	p, _ := consumer.Partitions(t)
	// 	log.Printf("list Topics: %s, Partitions %+v", t, p)

	// }

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	//log.Println("xxxxxxConsumePartition")
	if err != nil {
		log.Fatalf("ConsumePartition err: %+v", err)

		panic(err)
	}

	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	go func() {
		for err := range partitionConsumer.Errors() {
			fmt.Printf("partitionConsumer err: %+v", err)
		}
	}()
	//msg := <-partitionConsumer.Messages()
	//log.Println("Messages")
	//log.Printf("Consumed message body %s : ,%s\n", string(msg.Key), string(msg.Value))
	signals := make(chan os.Signal, 1)

	signal.Notify(signals, os.Interrupt)

	consumed := 0
	log.Println("ConsumerLoop")
	//ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			log.Printf("Consumed message body %s : ,%s\n", string(msg.Key), string(msg.Value))
			consumed++
			log.Printf("Consumed: %d\n", consumed)
		case <-signals:
			log.Printf("end Consumed: %d\n", consumed)
			//break ConsumerLoop
			return
		}
	}
}

func clusterPartConsumer() {
	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions

	// init consumer
	//brokers := []string{"192.168.21.225:9092"}
	topics := []string{topic}
	consumer, err := cluster.NewConsumer(addr, "my-consumer-group", topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case part, ok := <-consumer.Partitions():
			if !ok {
				return
			}

			go func(pc cluster.PartitionConsumer) {
				for msg := range pc.Messages() {
					fmt.Fprintf(os.Stdout, "Messaage: %s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
					consumer.MarkOffset(msg, "user data")
				}
			}(part)

		case <-signals:
			return

		}
	}
}

func clusterConsumer() {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	//brokers := []string{"192.168.21.225:9092"}
	topics := []string{topic}
	consumer, err := cluster.NewConsumer(addr, "testgroup", topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()

	// consume messages, watch signals
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				fmt.Fprintf(os.Stdout, "Messaage: %s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			return
		}
	}
}
