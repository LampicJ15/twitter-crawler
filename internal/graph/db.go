package graph

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"log"
)

type Database struct {
	driver neo4j.DriverWithContext
}

func SetupDb(ctx context.Context, uri string, username string, password string) Database {
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		log.Fatal(err)
	}
	connectivityError := driver.VerifyConnectivity(ctx)

	if connectivityError != nil {
		log.Fatal(connectivityError)
	}

	return Database{driver: driver}
}

func (database *Database) CloseDb(ctx context.Context) {
	database.driver.Close(ctx)
}

func (database *Database) NewSession(ctx context.Context, sessionConfig neo4j.SessionConfig) neo4j.SessionWithContext {
	return database.driver.NewSession(ctx, sessionConfig)
}
