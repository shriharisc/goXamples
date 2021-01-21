package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 1 {
		fmt.Println("Unexpected set of arguments. Expected subcommands..")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "producer":
		runProducer()

	case "consumer":
		runConsumer()

	default:
		fmt.Printf("Invalid command %s Expected either 'producer' or 'consumer' \n", os.Args[1])
		os.Exit(1)

	}
}
