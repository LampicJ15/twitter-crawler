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

func (client *twitterClient) GetUsersFollowing(ctx context.Context, id string, paginationToken string) (*twitter.UserFollowingLookupResponse, error) {
	lookUpOpts := twitter.UserFollowingLookupOpts{
		MaxResults:      1000,
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
	return response, err
}
