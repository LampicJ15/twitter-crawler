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

func setupSchema(db database, ctx context.Context) {
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

type importNode neo4j.Node
type importRelationship neo4j.Relationship

func (node importNode) importIt(ctx context.Context, tx neo4j.ExplicitTransaction) {
	_, err := tx.Run(ctx, `MERGE (node:_Import {_importId: $id})
						SET node += $properties
						WITH node
						CALL apoc.create.addLabels(node, $labels) YIELD node AS _
						RETURN *`,
		map[string]any{"id": node.Id, "labels": node.Labels, "properties": node.Props})

	if err != nil {
		log.Fatal(err)
	}
}

func (relationship importRelationship) importIt(ctx context.Context, tx neo4j.ExplicitTransaction) {
	_, err := tx.Run(ctx, `
						MERGE (startNode:_Import {_importId: $startId})
						MERGE (endNode:_Import {_importId: $endId})
						WITH startNode, endNode
						CALL apoc.merge.relationship(startNode, 
													$type,
													  {},
													  $properties,
													  endNode,
													  {}
													) YIELD rel RETURN rel
						`,
		map[string]any{"startId": relationship.StartId, "endId": relationship.EndId, "type": relationship.Type, "properties": relationship.Props})

	if err != nil {
		log.Fatal(err)
	}
}

type ImportInstructions struct {
	NodesFilePath         string
	RelationshipsFilePath string
}

func ImportGraph(importInstructions ImportInstructions, db database, ctx context.Context, batchSize int) {
	log.Println("Importing graph nodes and relationships.")

	log.Println("Setting up schema of the graph")
	setupSchema(db, ctx)

	log.Println("Setting up additional schema for faster import.")
	db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, "CREATE CONSTRAINT import_id IF NOT exists FOR (n:_Import) REQUIRE n._importId IS UNIQUE", map[string]any{})
	})

	log.Printf("Importig nodes from file %s.", importInstructions.NodesFilePath)
	importAll[importNode](importInstructions.NodesFilePath, db, ctx, batchSize)

	log.Println("Fixing node property types.")
	db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, `MATCH (n:TwitterAccount)
									SET n.createdAt = datetime(n.createdAt),
									n.verified = toBoolean(n.verified),
									n.isAccountPrivate = toBoolean(n.isAccountPrivate),
									n.tweetCount = toInteger(n.tweetCount),
									n.followersCount = toInteger(n.followersCount),
									n.followingsCount = toInteger(n.followingsCount)`, map[string]any{})
	})

	log.Printf("Finished import of nodes, starting with relationships from file %s.", importInstructions.RelationshipsFilePath)
	importAll[importRelationship](importInstructions.RelationshipsFilePath, db, ctx, batchSize)
	log.Println("Finished import of relationships.")

	log.Println("Cleaning up the additional schema.")
	db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, "MATCH (n:_Import) SET n._importId = NULL", map[string]any{})
	})

	db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, "MATCH (n:_Import) REMOVE n:_Import", map[string]any{})
	})

	db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, "DROP CONSTRAINT import_id IF EXISTS", map[string]any{})
	})
}

func importAll[T importer](filePath string, db database, ctx context.Context, batchSize int) {
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
			log.Printf("Reached the specified batch size of %d, commiting transaction.", batchSize)
			count = 0
			tx.Commit(ctx)
			tx, _ = session.BeginTransaction(ctx, neo4j.WithTxTimeout(60*time.Second))
		}
	}
}
