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

func Export(fileName string, db database, ctx context.Context) {
	exportNodes("nodes-"+fileName, db, ctx)
	ExportRelationships("relationships-"+fileName, db, ctx)
}

func exportNodes(filePath string, db database, ctx context.Context) {
	f, err := os.Create(filePath)
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
	log.Println("Export of graph nodes complete.")
}

func ExportRelationships(filePath string, db database, ctx context.Context) {
	f, err := os.Create(filePath)
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
	log.Println("Export of graph relationships complete.")
}
