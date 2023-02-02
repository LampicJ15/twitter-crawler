package graph

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"log"
)

type database struct {
	driver neo4j.DriverWithContext
}

func SetupDb(ctx context.Context, uri string, username string, password string) database {
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		log.Fatal(err)
	}
	connectivityError := driver.VerifyConnectivity(ctx)

	if connectivityError != nil {
		log.Fatal(connectivityError)
	}

	return database{driver: driver}
}

func (database *database) CloseDb(ctx context.Context) {
	database.driver.Close(ctx)
}

func (database *database) NewSession(ctx context.Context, sessionConfig neo4j.SessionConfig) neo4j.SessionWithContext {
	return database.driver.NewSession(ctx, sessionConfig)
}
