package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"time"
	
	servicebus "github.com/Azure/azure-service-bus-go"
)


func main() {
	// Run forever
	for {
		SubscribeToServiceBus()
	}
}

var sbName = "Endpoint=sb://************.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=***********************"
var qName = "queue-name"

func SubscribeToServiceBus() {

	const concurrentNum = 3
	msgChan := make(chan *servicebus.Message, concurrentNum)
	
	// Define a function that should be executed when a message is received.
	var concurrentHandler servicebus.HandlerFunc = func(ctx context.Context, msg *servicebus.Message) error {
		msgChan <- msg
		return nil
	}

	// Define msg workers
	for i := 0; i < concurrentNum; i++ {
		go func() {
			for msg := range msgChan {

				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				defer cancel()
				fmt.Print(string(msg.Data))
				//if ok := Process(msg); ok {
				//	msg.Complete(ctx)
				//} else {
				//	msg.Abandon(ctx)
				//}
			}
		}()
	}

	// Instantiate the clients needed to communicate with a Service Bus Queue.
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(sbName))
	if err != nil {
		return
	}

	// Define a context to limit how long we will block to receive messages, then start serving our function.
	// Init queue client with prefetch count
	client, err := ns.NewQueue(qName, servicebus.QueueWithPrefetchCount(concurrentNum))
	if err != nil {
		close(msgChan)
		return
	}

	// Define a context to limit how long we will block to receive messages, then start serving our function.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := client.Receive(ctx, concurrentHandler); err != nil {
		log.Println("Info: ", err)
	}
	// Close the message chan
	close(msgChan)
}


