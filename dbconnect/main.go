package main

import (
	"fmt"

	"github.com/magiconair/properties"
)

func main() {
	err := run(GetConnectURL("${PWD}/config.properties"))

	if err != nil {
		panic(err)
	}
}

//GetConnectURL - gets/constructs the db connect url from properties
func GetConnectURL(propertyfile string) string {
	p := properties.MustLoadFile(propertyfile, properties.UTF8)

	return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		p.MustGetString("host"), p.MustGetInt("port"), p.MustGetString("username"), p.MustGetString("password"),
		p.MustGetString("dbname"))
}
