package twitter

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"log"
	"twitter-crawler/internal/graph"
)

type crawlUser struct {
	id       string
	username string
}

func setupCrawler(ctx context.Context, db graph.Database) {
	log.Println("Setting up additional constraints for more efficient crawling.")
	db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, "CREATE INDEX crawled IF NOT EXISTS FOR (n:Crawl) ON (n._importedFollowing)", map[string]any{})
	})
}

func getNextUserToCrawl(ctx context.Context, db graph.Database) {
	db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead}).ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `CALL {
							MATCH (user:_Crawl {_importedFollowing: false})
							RETURN user LIMIT 1
						
							UNION
						
							MATCH (user:TwitterAccount)
							  WHERE NOT (user:_Crawl OR user.isAccountPrivate) AND (user.followersCount > 100)
							RETURN user ORDER BY user._referenceScore DESC LIMIT 1
							}
							RETURN user.id AS id, user.username AS username`, map[string]any{})

		if err != nil {
			return crawlUser{}, err
		}

		record, err := result.Single(ctx)
		userId, _ := record.Get("id")
		username, _ := record.Get("username")
		return crawlUser{id: userId.(string), username: username.(string)}, err
	})
}
