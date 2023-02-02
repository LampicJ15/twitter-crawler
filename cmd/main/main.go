package main

import (
	"context"
	"log"
	"twitter-crawler/internal/graph"
)

func main() {
	log.Println("Connecting to the graph database")
	ctx := context.Background()
	database := graph.SetupDb(ctx, "neo4j://localhost:7687", "neo4j", "test")
	defer database.CloseDb(ctx)

	//graph.Export("graph.jsonl", database, ctx)
	graph.ImportAll[graph.ImportNode]("resources/nodes.jsonl", database, ctx, 250)
}
