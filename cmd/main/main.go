package main

import (
	"context"
	"log"
	"twitter-crawler/internal/graph"
	"twitter-crawler/internal/twitter"
)

func main() {
	log.Println("Connecting to the graph database")
	ctx := context.Background()
	database := graph.SetupDb(ctx, "neo4j://localhost:7687", "neo4j", "test")
	defer database.CloseDb(ctx)

	graph.Export("graph.jsonl", database, ctx)
	graph.ImportGraph(graph.ImportInstructions{NodesFilePath: "resources/nodes.jsonl", RelationshipsFilePath: "resources/relationships.jsonl"}, database, ctx, 1000)
	client := twitter.SetupClient("")
	twitter.Crawl(ctx, database, client)
}
