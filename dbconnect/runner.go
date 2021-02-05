package main

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/lib/pq"
)

//Job structure to hold job records.
type Job struct {
	ID      uint
	Name    string
	Payload string
	RunAt   time.Time
}

//run the main program loop
func run(connecturl string) error {
	log.Print("Connecting over db-url - ", connecturl)

	db, err := InitDBConnections(connecturl)
	if err != nil {
		return err
	}
	defer db.Close()

	err = CreateJobSchema(db)
	if err != nil {
		return err
	}

	err = InsertANewJob(db, "Try Something Better!", "test1@test.com", time.Now())
	if err != nil {
		return err
	}

	jobs, err1 := QueryForJobs(db)
	if err1 != nil {
		return err1
	}
	log.Println("Scanning all jobs...")
	for _, job := range jobs {
		log.Println("Job with ID ", job.ID, ",name= ", job.Name, ",Payload= ", job.Payload, ", to-RunAt= ", job.RunAt)
	}
	return nil
}

// InitDBConnections - initialize db connection
func InitDBConnections(conn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Panic("Couldnt connected to database...", err)
		return nil, err
	}
	return db, nil
}

//CreateJobSchema - creates the app tables needed.
func CreateJobSchema(db *sql.DB) error {
	log.Print("Creating Job schema...")
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS "public"."jobs"(
				"id" SERIAL PRIMARY KEY,
				"name" varchar(50) not null,
				"payload" text,
				"runAt" TIMESTAMP NOT NULL,
				"cron" varchar(50) DEFAULT '-')`)
	if err != nil {
		log.Panic("Error executing db definition...", err)
		return err
	}
	return nil
}

//InsertANewJob - inserts a new job given the details
func InsertANewJob(db *sql.DB, name string, payload string, runAt time.Time) error {
	log.Print("inserting a new job...")
	_, err := db.Exec(`INSERT INTO "public"."jobs" ("name", "payload", "runAt") VALUES ($1,$2,$3)`,
		name, payload, runAt)
	if err != nil {
		log.Fatal("Error inserting a new job", err)
		return err
	}
	return nil
}

//QueryForJobs - queries for jobs whose runAt is configured in the past
func QueryForJobs(db *sql.DB) ([]Job, error) {
	jobs := []Job{}
	log.Print("Querying jobs schema...")
	rows, err := db.Query(`SELECT "id","name","payload","runAt" FROM "public"."jobs" WHERE "runAt" < $1`, time.Now())
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		job := Job{}
		rows.Scan(&job.ID, &job.Name, &job.Payload, &job.RunAt)
		jobs = append(jobs, job)
	}
	return jobs, nil
}
