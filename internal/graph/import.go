package graph

import (
	"bufio"
	"context"
	"encoding/json"
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

func setupSchema(db database, ctx context.Context) {
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

type importer interface {
	importIt(ctx context.Context, tx neo4j.ExplicitTransaction)
}

type ImportNode neo4j.Node
type ImportRelationship neo4j.Relationship

func (node ImportNode) importIt(ctx context.Context, tx neo4j.ExplicitTransaction) {
	_, err := tx.Run(ctx, `MERGE (node {_id: $id})
						SET node += $properties
						WITH node
						CALL apoc.create.setLabels(node, $labels) YIELD node AS _
						RETURN *`,
		map[string]any{"id": node.Id, "labels": node.Labels, "properties": node.Props})

	if err != nil {
		log.Fatal(err)
	}
}

func (relationship ImportRelationship) importIt(ctx context.Context, tx neo4j.ExplicitTransaction) {
	return
}

func ImportGraph(filePath string, db database, ctx context.Context, batchSize int) {
	log.Println("Importing graph nodes and relationships.")
	log.Println("Setting up additional schema for faster import.")

	// todo: add separate label for import
	log.Printf("Importig nodes from file %s", filePath)
	ImportAll[ImportNode](filePath, db, ctx, batchSize)
	log.Println("Finished import of nodes, starting with relationships.")
	ImportAll[ImportRelationship](filePath, db, ctx, batchSize)
	log.Println("Finished import of relationships.")

	log.Println("Cleaning up the additional schema.")
	// todo: remove additional labels and properties from the db
}

func ImportAll[T importer](filePath string, db database, ctx context.Context, batchSize int) {
	readFile, err := os.Open(filePath)
	defer readFile.Close()

	if err != nil {
		log.Fatal(err)
	}

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	session := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	tx, _ := session.BeginTransaction(ctx, neo4j.WithTxTimeout(60*time.Second))

	var count int
	for fileScanner.Scan() {
		var graphObject T
		json.Unmarshal([]byte(fileScanner.Text()), &graphObject)

		graphObject.importIt(ctx, tx)

		count++
		if count == batchSize {
			log.Printf("Reached the specified batch size of %d, commiting transaction", batchSize)
			count = 0
			tx.Commit(ctx)
			tx, _ = session.BeginTransaction(ctx, neo4j.WithTxTimeout(60*time.Second))
		}
	}
}
