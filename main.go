package main

import (
	"context"
	"fmt"

	"database/sql"

	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

func handleRequest(ctx context.Context, e events.DynamoDBEvent) {

	host := "database-1.ctm1ks5phyah.us-east-2.rds.amazonaws.com"
	port := 5432
	user := "postgres"
	passwd := "postgres"
	dbName := "test"
	tableName := "JsonData"
	var db *sql.DB
	var err error

	// Connecting to the database
	dbinfo := fmt.Sprintf("host=%s port=%d user=%s password=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, passwd, dbName)

	if db, err = sql.Open("postgres", dbinfo); err != nil {
		fmt.Printf("Error connecting to the database")
	}

	for _, record := range e.Records {
		fmt.Printf("Processing request data for event ID %s, type %s.\n", record.EventID, record.EventName)
		b, err := json.Marshal(record)
		if err != nil {
			fmt.Printf("Error: %s", err)
			return
		}

		_, err = db.Exec("INSERT INTO "+tableName+"(data) VALUES($1);", string(b))
		if err != nil {
			fmt.Printf("Error executing query %v", err)
		}

	}
}

func main() {
	lambda.Start(handleRequest)
}
