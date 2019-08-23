package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"database/sql"

	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/lib/pq"
)

func handleRequest(ctx context.Context, e events.DynamoDBEvent) {

	prop, _ := ReadPropertiesFile("./properties.ini")
	host := prop["host"]
	user := prop["user"]
	passwd := prop["passwd"]
	dbName := prop["dbName"]
	tableName := prop["tableName"]
	port, _ := strconv.Atoi(prop["port"])

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

type AppConfigProperties map[string]string

func ReadPropertiesFile(filename string) (AppConfigProperties, error) {
	config := AppConfigProperties{}

	if len(filename) == 0 {
		return config, nil
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if equal := strings.Index(line, "="); equal >= 0 {
			if key := strings.TrimSpace(line[:equal]); len(key) > 0 {
				value := ""
				if len(line) > equal {
					value = strings.TrimSpace(line[equal+1:])
				}
				config[key] = value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
		return nil, err
	}

	return config, nil
}
