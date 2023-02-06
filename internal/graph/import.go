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

func setupSchema(db Database, ctx context.Context) {
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

type mappable interface {
	toMap() map[string]any
}

type node neo4j.Node
type relationship neo4j.Relationship

func (n node) toMap() map[string]any {
	return map[string]any{"labels": n.Labels, "id": n.Id, "properties": n.Props}
}

func importWithCypherQuery(ctx context.Context, tx neo4j.ExplicitTransaction, query string, entities []map[string]any) {
	_, err := tx.Run(ctx, query,
		map[string]any{"entities": entities})

	if err != nil {
		log.Fatal(err)
	}
}

func (rel relationship) toMap() map[string]any {
	return map[string]any{"type": rel.Type, "startId": rel.StartId, "endId": rel.EndId, "properties": rel.Props}
}

type ImportInstructions struct {
	NodesFilePath         string
	RelationshipsFilePath string
}

func ImportGraph(importInstructions ImportInstructions, db Database, ctx context.Context, batchSize int) {
	log.Println("Importing graph nodes and relationships.")

	log.Println("Setting up schema of the graph")
	setupSchema(db, ctx)

	log.Println("Setting up additional schema for faster import.")
	db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, "CREATE CONSTRAINT import_id IF NOT exists FOR (n:_Import) REQUIRE n._importId IS UNIQUE", map[string]any{})
	})

	log.Printf("Importig nodes from file %s.", importInstructions.NodesFilePath)
	importAll[node](importInstructions.NodesFilePath, db, ctx, batchSize, `UNWIND $entities AS entity
																				 MERGE (node:_Import {_importId: entity.id})
																				 SET node += entity.properties
																				 WITH node, entity
																				 CALL apoc.create.addLabels(node, entity.labels) YIELD node AS _
																				 RETURN *`)

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
	importAll[relationship](importInstructions.RelationshipsFilePath, db, ctx, batchSize, `UNWIND $entities AS entity
																								 MATCH (startNode:_Import {_importId: entity.startId})
																								 MATCH (endNode:_Import {_importId: entity.endId})
																								 WITH entity, startNode, endNode
																								 CALL apoc.merge.relationship(startNode, entity.type, {}, entity.properties, endNode, {}) YIELD rel RETURN rel`)
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

func importAll[T mappable](filePath string, db Database, ctx context.Context, batchSize int, query string) {
	readFile, err := os.Open(filePath)
	defer readFile.Close()

	if err != nil {
		log.Fatal(err)
	}

	fileScanner := bufio.NewScanner(readFile)
	fileScanner.Split(bufio.ScanLines)

	session := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	tx, _ := session.BeginTransaction(ctx, neo4j.WithTxTimeout(60*time.Second))

	var graph []map[string]any
	for fileScanner.Scan() {
		var graphEntity T
		json.Unmarshal([]byte(fileScanner.Text()), &graphEntity)
		graph = append(graph, graphEntity.toMap())

		if batchSize == len(graph) {
			log.Printf("Reached the specified batch size of %d, commiting transaction.", batchSize)
			importWithCypherQuery(ctx, tx, query, graph)
			tx.Commit(ctx)
			tx, _ = session.BeginTransaction(ctx, neo4j.WithTxTimeout(60*time.Second))
			graph = nil
		}
	}
	importWithCypherQuery(ctx, tx, query, graph)
	tx.Commit(ctx)
}
