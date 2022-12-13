package fireboltgosdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/astaxie/beego/cache"
)

type AuthenticationResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	TokenType    string `json:"token_type"`
	Scope        string `json:"scope"`
}

var tokenCache cache.Cache

func init() {
	var err error
	tokenCache, err = cache.NewCache("memory", `{}`)
	if err != nil {
		infolog.Println(fmt.Errorf("could not create memory cache to store access tokens: %v", err))
	}
}

// Authenticate sends an authentication request, and returns a newly constructed client object
func Authenticate(username, password, apiEndpoint string) (*Client, string, error) {
	var loginUrl string
	var contentType string
	var body string
	var err error

	userAgent := ConstructUserAgentString()
	cachedToken := getCachedAccessToken(username, apiEndpoint)
	if len(cachedToken) > 0 {
		return &Client{Username: username, Password: password, ApiEndpoint: apiEndpoint, UserAgent: userAgent}, cachedToken, nil
	} else {
		if isServiceAccount(username) {
			loginUrl, contentType, body, err = prepareUsernamePasswordLogin(username, password)
			if err != nil {
				return nil, "", err
			}
		} else {
			loginUrl, contentType, body = prepareServiceAccountLogin(username, password)
		}
		infolog.Printf("Start authentication into '%s' using '%s'", apiEndpoint, loginUrl)

		resp, err, _ := request(context.TODO(), "", "POST", apiEndpoint+loginUrl, userAgent, nil, body, contentType)
		if err != nil {
			return nil, "", ConstructNestedError("authentication request failed", err)
		}

		var authResp AuthenticationResponse
		if err = jsonStrictUnmarshall(resp, &authResp); err != nil {
			return nil, "", ConstructNestedError("failed to unmarshal authentication response with error", err)
		}

		infolog.Printf("Authentication was successful")
		err = tokenCache.Put(getCacheKey(username, apiEndpoint), authResp.AccessToken, time.Duration(authResp.ExpiresIn)*time.Millisecond)
		if err != nil {
			infolog.Println(fmt.Errorf("failed to cache access token: %v", err))
		}
		return &Client{Username: username, Password: password, ApiEndpoint: apiEndpoint, UserAgent: userAgent}, authResp.AccessToken, nil
	}
}

// getCacheKey calculates an access token key using the username and the apiEndpoint provided
func getCacheKey(username, apiEndpoint string) string {
	return username + apiEndpoint
}

// getCachedAccessToken returns a cached access token or empty when a token could not be found
func getCachedAccessToken(username, apiEndpoint string) string {
	var cachedToken = tokenCache.Get(getCacheKey(username, apiEndpoint))
	if cachedToken != nil {
		return fmt.Sprint(cachedToken)
	} else {
		return ""
	}
}

func prepareUsernamePasswordLogin(username string, password string) (string, string, string, error) {
	var authUrl = UsernamePasswordURLSuffix
	//var values = map[string]string{"username": username, "password": password}
	values := map[string]string{"username": username, "password": password}
	jsonData, err := json.Marshal(values)
	if err != nil {
		return "", "", "", ConstructNestedError("error during json marshalling", err)
	} else {
		return authUrl, ContentTypeJSON, string(jsonData), nil
	}
}

func prepareServiceAccountLogin(username, password string) (string, string, string) {
	var authUrl = ServiceAccountLoginURLSuffix
	form := url.Values{}
	form.Add("client_id", username)
	form.Add("client_secret", password)
	form.Add("grant_type", "client_credentials")
	var body = form.Encode()
	return authUrl, ContentTypeForm, body
}

// deleteAccessTokenFromCache deletes an access token from the cache if available
func deleteAccessTokenFromCache(username, apiEndpoint string) {
	err := tokenCache.Delete(getCacheKey(username, apiEndpoint))
	if err != nil {
		infolog.Println(fmt.Errorf("could not remove token from the memory cache: %v", err))
	}
}

// isServiceAccount checks if a username is a service account cliend id
func isServiceAccount(username string) bool {
	return strings.Contains(username, "@")
}
