package twitter

import (
	"context"
	"github.com/g8rswimmer/go-twitter/v2"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"log"
	"time"
	"twitter-crawler/internal/graph"
)

type crawlUser struct {
	id              string
	username        string
	paginationToken string
}

func setupGraphForCrawling(ctx context.Context, db graph.Database) {
	log.Println("Setting up additional constraints for more efficient crawling.")
	_, err := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, "CREATE INDEX crawled IF NOT EXISTS FOR (n:_Crawl) ON (n._importedFollowing)", map[string]any{})
	})
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, "CREATE INDEX crawled IF NOT EXISTS FOR (n:TwitterAccount) ON (n._referenceScore)", map[string]any{})
	})

	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Calculate and define refernce scores.")
	_, err = db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, `
							MATCH (twitter:TwitterAccount)
							WHERE NOT twitter.followersCount = 0
							OPTIONAL MATCH (twitter)<-[f:FOLLOWS]-()
							WITH twitter, count(f) AS followedInGraph
							SET twitter._referenceScore = toFloat(followedInGraph) / twitter.followersCount
							`, map[string]any{})
	})

	if err != nil {
		log.Fatal(err)
	}
}

func getNextUserToCrawl(ctx context.Context, db graph.Database) crawlUser {
	user, _ := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead}).ExecuteRead(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		result, err := tx.Run(ctx, `CALL {
											MATCH (user:_Crawl {_importedFollowing: false})
											RETURN user LIMIT 1
										
											UNION
										
											MATCH (user:TwitterAccount)
											  WHERE NOT (user:_Crawl OR user.isAccountPrivate) AND (user.followersCount > 1000)
											RETURN user ORDER BY user._referenceScore DESC LIMIT 1
											}
											RETURN user.id AS id, user.username AS username, CASE WHEN user._paginationToken IS NULL THEN "" ELSE user._paginationToken END  AS paginationToken LIMIT 1`, map[string]any{})

		if err != nil {
			log.Fatal(err)
		}

		record, err := result.Single(ctx)
		userId, _ := record.Get("id")
		username, _ := record.Get("username")
		token, _ := record.Get("paginationToken")
		return crawlUser{id: userId.(string), username: username.(string), paginationToken: token.(string)}, err
	})

	return user.(crawlUser)
}

func toSliceOfMaps(response *twitter.UserFollowingLookupResponse) []map[string]any {
	var followings []map[string]any
	for _, user := range response.Raw.Users {
		followings = append(followings, map[string]any{
			"username":         user.UserName,
			"name":             user.Name,
			"id":               user.ID,
			"createdAt":        user.CreatedAt,
			"followersCount":   user.PublicMetrics.Followers,
			"followingsCount":  user.PublicMetrics.Following,
			"tweetCount":       user.PublicMetrics.Tweets,
			"isAccountPrivate": user.Protected,
			"profileImageUrl":  user.ProfileImageURL,
			"location":         user.ProfileImageURL,
			"verified":         user.Verified,
			"url":              user.URL,
		})
	}
	return followings
}

func storeUserFollowing(ctx context.Context, db graph.Database, user crawlUser, followingResponse *twitter.UserFollowingLookupResponse) {
	following := toSliceOfMaps(followingResponse)
	paginationToken := followingResponse.Meta.NextToken
	_, err := db.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}).ExecuteWrite(ctx, func(tx neo4j.ManagedTransaction) (any, error) {
		return tx.Run(ctx, `
									WITH $userId AS userId, $token AS token, $entities AS entities
									MERGE (user:TwitterAccount {id: userId})
									SET user:_Crawl, 
 										user._paginationToken = token, 
										user._importedFollowing = CASE WHEN token = '' 
																		THEN true 
																	    ELSE false END

									WITH user, entities
									UNWIND entities AS entity
									WITH user, entity
									
									MERGE (followedUser:TwitterAccount {id: entity.id})
									  ON CREATE SET followedUser += {
										name:              entity.name,
										username:          entity.username,
										createdAt:         datetime(entity.created_at),
										followersCount:    entity.public_metrics.followers_count,
										followingsCount:   entity.public_metrics.following_count,
										tweetCount:        entity.public_metrics.tweet_count,
										isAccountPrivate:  entity.protected,
										profileImageUrl:   entity.profile_image_url,
										location:          entity.location,
										verified:          entity. verified,
										url:               entity.url,
										_referenceScore:   0.0,
										_importedFollowing: false
									  }

									WITH user, followedUser
									MERGE (user)-[:FOLLOWS]->(followedUser)
									ON CREATE SET followedUser._referenceScore = followedUser._referenceScore + 1.0 / followedUser.followersCount
									`,
			map[string]any{"userId": user.id, "entities": following, "token": paginationToken})
	})

	if err != nil {
		log.Fatal(err)
	}
}

func Crawl(ctx context.Context, db graph.Database, client twitterClient) {
	setupGraphForCrawling(ctx, db)
	for {
		user := getNextUserToCrawl(ctx, db)
		log.Printf("Crawling user with id %s, username %s and pagination token %s.", user.id, user.username, user.paginationToken)
		response, err := client.GetUsersFollowing(ctx, user.id, user.paginationToken)

		if err != nil {
			log.Println(err)
			log.Println("Sleeping for 15 minutes.")
			time.Sleep(15 * time.Minute)
			continue
		}

		storeUserFollowing(ctx, db, user, response)
	}
}
