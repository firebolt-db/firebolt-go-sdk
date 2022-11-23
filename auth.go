package fireboltgosdk

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

type AuthenticationResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

var cache = ttlcache.New[string, string]()

// Authenticate sends an authentication request when a token is not available in the cache, and returns a newly constructed client object
func Authenticate(username, password, apiEndpoint string) (*Client, error) {
	infolog.Printf("Start authentication into '%s' using '%s'", apiEndpoint, LoginUrl)
	userAgent := ConstructUserAgentString()
	cachedToken := getCachedAccessToken(username, apiEndpoint)
	if len(cachedToken) > 0 {
		infolog.Printf("Returning auth token from cache")
		return &Client{Username: username, Password: password, ApiEndpoint: apiEndpoint, UserAgent: userAgent}, nil
	} else {
		values := map[string]string{"username": username, "password": password}
		jsonData, err := json.Marshal(values)
		if err != nil {
			return nil, ConstructNestedError("error during json marshalling", err)
		}
		resp, err, _ := request(context.TODO(), "", "POST", apiEndpoint+LoginUrl, userAgent, nil, string(jsonData))
		if err != nil {
			return nil, ConstructNestedError("authentication request failed", err)
		}

		var authResp AuthenticationResponse
		if err = jsonStrictUnmarshall(resp, &authResp); err != nil {
			return nil, ConstructNestedError("failed to unmarshal authentication response with error", err)
		}

		infolog.Printf("Authentication was successful")
		cache.Set(getCacheKey(username, apiEndpoint), authResp.AccessToken, time.Duration(authResp.ExpiresIn)*time.Millisecond)
		return &Client{Username: username, Password: password, ApiEndpoint: apiEndpoint, UserAgent: userAgent}, nil
	}
}

// getCacheKey calculates an access token key using the username and the apiEndpoint provided
func getCacheKey(username, apiEndpoint string) string {
	return username + apiEndpoint
}

// getCachedAccessToken returns a cached access token or empty when a token could not be found
func getCachedAccessToken(username, apiEndpoint string) string {
	cached := cache.Get(getCacheKey(username, apiEndpoint))
	if cached != nil {
		return cached.Value()
	} else {
		return ""
	}
}

// deleteAccessTokenFromCache deletes an access token from the cache if available
func deleteAccessTokenFromCache(username, apiEndpoint string) {
	cache.Delete(getCacheKey(username, apiEndpoint))
}
