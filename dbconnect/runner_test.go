package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
)

var db *sql.DB
var err error

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not create a new docker pool - %s", err)
	}
	resource, err := pool.Run("postgres", "12", []string{"POSTGRES_PASSWORD=secret", "POSTGRES_DB=tstconnect"})
	if err != nil {
		log.Fatalf("Could not create a resource on pool - %s", err)
	}

	if err = pool.Retry(func() error {
		var err error
		db, err = sql.Open("postgres", fmt.Sprintf("postgres://postgres:secret@localhost:%s/%s?sslmode=disable", resource.GetPort("5432/tcp"), "tstconnect"))
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker pool - %s", err)
	}

	code := m.Run()

	//Defering this logic isnt possible as os.Exit(xxx) doesnt care about defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Unable to clean resources - %s", err)
	}
	os.Exit(code)
}

func TestFullCreateInsertQueryJob(t *testing.T) {
	err := CreateJobSchema(db)
	if err != nil {
		t.Fatalf("Unable to create job-schema... %s", err)
	}
	err = InsertANewJob(db, "A Test Job", "you@meattest.com", time.Now())
	if err != nil {
		t.Fatalf("Unable to insert a new test job.. %s", err)
	}
	jobs, err := QueryForJobs(db)
	if err != nil {
		t.Fatalf("Unable to query jobs... %s", err)
	}
	for _, job := range jobs {
		log.Printf("Job name %s", job.Name)
	}
}

func TestGetConnectURL(t *testing.T) {
	connecturl := GetConnectURL("${PWD}/config.properties")
	assert.Equal(t, connecturl,
		"host=localhost port=5432 user=postgres password=fedora dbname=connect sslmode=disable",
		"looks like connect-url is not correct!")
}

func TestInitDBConnection(t *testing.T) {
	db, _ := InitDBConnections("host=localhost port=5432 user=postgres password=fedora dbname=connect sslmode=disable")
	assert.NotNil(t, db)
}
