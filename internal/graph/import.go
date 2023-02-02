package graph

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"log"
	"os"
	"time"
)

var constraints = [...]string{
	"CREATE CONSTRAINT person_name IF NOT exists FOR (person:Person) REQUIRE person.name IS UNIQUE",
	"CREATE CONSTRAINT political_party_name IF NOT exists FOR (party:Party) REQUIRE party.name IS UNIQUE",
	"CREATE CONSTRAINT twitter_user_id IF NOT exists FOR (user:TwitterAccount) REQUIRE user.id IS UNIQUE",
	"CREATE CONSTRAINT twitter_username IF NOT exists FOR (user:TwitterAccount) REQUIRE user.username IS UNIQUE"}

const graphData = "resources/graph.jsonl"

func SetupSchema(db database, ctx context.Context) {
	log.Println("Setting up graph schema")
	session := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	tx, _ := session.BeginTransaction(ctx, neo4j.WithTxTimeout(5*time.Second))

	for _, constraint := range constraints {
		_, err := tx.Run(ctx, constraint, map[string]any{})
		if err != nil {
			tx.Rollback(ctx)
			log.Fatal(err)
		}
	}
	tx.Commit(ctx)
}

func Export(db database, ctx context.Context) {
	exportNodes(db, ctx)
	ExportRelationships(db, ctx)
}

func exportNodes(db database, ctx context.Context) {
	log.Println("Creating graph file.")
	f, err := os.Create("resources/nodes.jsonl")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	session := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	tx, _ := session.BeginTransaction(ctx, neo4j.WithTxTimeout(60*time.Second))

	log.Println("Exporting graph nodes.")
	result, _ := tx.Run(ctx, "MATCH (node) RETURN node", map[string]any{})
	for result.Next(ctx) {
		record := result.Record()
		result, _ := record.Get("node")
		resultNode := result.(neo4j.Node)
		b, err := json.Marshal(resultNode)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, err2 := f.WriteString(string(b) + "\n")
		if err2 != nil {
			log.Fatal(err2)
		}
	}

	tx.Close(ctx)
}

func ExportRelationships(db database, ctx context.Context) {
	log.Println("Creating graph file.")
	f, err := os.Create("resources/relationships.jsonl")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	session := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	tx, _ := session.BeginTransaction(ctx, neo4j.WithTxTimeout(60*time.Second))
	log.Println("Exporting graph relationships.")
	result, _ := tx.Run(ctx, "MATCH ()-[relation]->() RETURN relation", map[string]any{})
	for result.Next(ctx) {
		record := result.Record()
		result, _ := record.Get("relation")
		resultRel := result.(neo4j.Relationship)
		b, err := json.Marshal(resultRel)
		if err != nil {
			fmt.Println(err)
			return
		}
		_, err2 := f.WriteString(string(b) + "\n")
		if err2 != nil {
			log.Fatal(err2)
		}
	}
	tx.Close(ctx)
}
