package twitter

import (
	"context"
	"fmt"
	"github.com/g8rswimmer/go-twitter/v2"
	"log"
	"net/http"
)

type authorize struct {
	Token string
}

func (a authorize) Add(req *http.Request) {
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", a.Token))
}

type twitterClient struct {
	twitter *twitter.Client
}

func SetupClient(apiToken string) twitterClient {
	return twitterClient{twitter: &twitter.Client{
		Authorizer: authorize{
			Token: apiToken,
		},
		Client: http.DefaultClient,
		Host:   "https://api.twitter.com",
	}}
}

// user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld&
//tweet.fields=attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,non_public_metrics,organic_metrics,possibly_sensitive,promoted_metrics,public_metrics,referenced_tweets,reply_settings,source,text,withheld'
func (client *twitterClient) LookupUsersByUsernames(ctx context.Context, usernames []string) {
	lookUpOpts := twitter.UserLookupOpts{UserFields: []twitter.UserField{twitter.UserFieldCreatedAt,
		twitter.UserFieldDescription,
		twitter.UserFieldEntities,
		twitter.UserFieldID,
		twitter.UserFieldLocation,
		twitter.UserFieldName,
		twitter.UserFieldUserName,
		twitter.UserFieldProfileImageURL,
		twitter.UserFieldProtected,
		twitter.UserFieldVerified,
		twitter.UserFieldPublicMetrics}}
	response, err := client.twitter.UserNameLookup(ctx, usernames, lookUpOpts)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(response.Raw.Users[0].PublicMetrics.Tweets)
}

func (client *twitterClient) GetUsersFollowing(ctx context.Context, id string, paginationToken string) (users []*twitter.UserObj, nextToken string, err error) {
	lookUpOpts := twitter.UserFollowingLookupOpts{
		PaginationToken: paginationToken,
		UserFields: []twitter.UserField{
			twitter.UserFieldCreatedAt,
			twitter.UserFieldDescription,
			twitter.UserFieldEntities,
			twitter.UserFieldID,
			twitter.UserFieldLocation,
			twitter.UserFieldName,
			twitter.UserFieldUserName,
			twitter.UserFieldProfileImageURL,
			twitter.UserFieldProtected,
			twitter.UserFieldVerified,
			twitter.UserFieldURL,
			twitter.UserFieldPublicMetrics}}
	response, err := client.twitter.UserFollowingLookup(ctx, id, lookUpOpts)
	return response.Raw.Users, response.Meta.NextToken, err
}
