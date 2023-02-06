package main

import (
	"context"
	"flag"
	"log"
	"twitter-crawler/internal/graph"
	"twitter-crawler/internal/twitter"
)

func main() {
	log.Println("Connecting to the graph database")
	ctx := context.Background()
	database := graph.SetupDb(ctx, "neo4j://localhost:7687", "neo4j", "test")
	defer database.CloseDb(ctx)

	action := flag.String("action", "", "defines action to be executed: import, export or crawl")
	authorizationToken := flag.String("token", "", "bearer token for twitter authentication")
	flag.Parse()

	switch *action {
	case "crawl":
		{
			if *authorizationToken == "" {
				log.Fatal("an authorization token for the twitter client must be provided to start crawling.")
			}
			client := twitter.SetupClient(*authorizationToken)
			twitter.Crawl(ctx, database, client)
		}
	case "import":
		graph.ImportGraph(graph.ImportInstructions{NodesFilePath: "resources/nodes.jsonl", RelationshipsFilePath: "resources/relationships.jsonl"}, database, ctx, 1000)
	case "export":
		graph.Export("graph.jsonl", database, ctx)
	default:
		log.Fatal("none of the possible actions was provided, please provide one of the following: crawl, import, export")
	}
}
