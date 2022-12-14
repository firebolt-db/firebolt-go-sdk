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
func Authenticate(username, password, apiEndpoint string) (*Client, error) {
	userAgent := ConstructUserAgentString()
	_, err := getAccessToken(username, password, apiEndpoint, userAgent)
	if err != nil {
		return nil, ConstructNestedError("error while getting access token", err)
	} else {
		return &Client{Username: username, Password: password, ApiEndpoint: apiEndpoint, UserAgent: userAgent}, nil
	}
}

// getAccessToken gets an access token from the cache when it is available in the cache or from the server when it is not available in the cache
func getAccessToken(username string, password string, apiEndpoint string, userAgent string) (string, error) {
	cachedToken := getCachedAccessToken(username, apiEndpoint)
	if len(cachedToken) > 0 {
		return cachedToken, nil
	} else {
		var loginUrl string
		var contentType string
		var body string
		var err error
		if isServiceAccount(username) {
			loginUrl, contentType, body = prepareServiceAccountLogin(username, password)
		} else {
			loginUrl, contentType, body, err = prepareUsernamePasswordLogin(username, password)
			if err != nil {
				return "", err
			}
		}
		infolog.Printf("Start authentication into '%s' using '%s'", apiEndpoint, loginUrl)
		resp, err, _ := request(context.TODO(), "", "POST", apiEndpoint+loginUrl, userAgent, nil, body, contentType)
		if err != nil {
			return "", ConstructNestedError("authentication request failed", err)
		}

		var authResp AuthenticationResponse
		if err = jsonStrictUnmarshall(resp, &authResp); err != nil {
			return "", ConstructNestedError("failed to unmarshal authentication response with error", err)
		}
		infolog.Printf("Authentication was successful")
		if tokenCache != nil {
			err = tokenCache.Put(getCacheKey(username, apiEndpoint), authResp.AccessToken, time.Duration(authResp.ExpiresIn)*time.Millisecond)
			if err != nil {
				infolog.Println(fmt.Errorf("failed to cache access token: %v", err))
			}
		}
		return authResp.AccessToken, nil
	}
}

// getCacheKey calculates an access token key using the username and the apiEndpoint provided
func getCacheKey(username, apiEndpoint string) string {
	return username + apiEndpoint
}

// getCachedAccessToken returns a cached access token or empty when a token could not be found
func getCachedAccessToken(username, apiEndpoint string) string {
	if tokenCache != nil {
		var cachedToken = tokenCache.Get(getCacheKey(username, apiEndpoint))
		if cachedToken != nil {
			return fmt.Sprint(cachedToken)
		}
	}
	return ""
}

// prepareUsernamePasswordLogin returns the loginUrl, contentType and body needed to query an access token using a username and a password
func prepareUsernamePasswordLogin(username string, password string) (string, string, string, error) {
	var authUrl = UsernamePasswordURLSuffix
	values := map[string]string{"username": username, "password": password}
	jsonData, err := json.Marshal(values)
	if err != nil {
		return "", "", "", ConstructNestedError("error during json marshalling", err)
	} else {
		return authUrl, ContentTypeJSON, string(jsonData), nil
	}
}

// prepareServiceAccountLogin returns the loginUrl, contentType and body needed to query an access token using a client id and a client secret
func prepareServiceAccountLogin(clientId, clientSecret string) (string, string, string) {
	var authUrl = ServiceAccountLoginURLSuffix
	form := url.Values{}
	form.Add("client_id", clientId)
	form.Add("client_secret", clientSecret)
	form.Add("grant_type", "client_credentials")
	var body = form.Encode()
	return authUrl, ContentTypeForm, body
}

// deleteAccessTokenFromCache deletes an access token from the cache if available
func deleteAccessTokenFromCache(username, apiEndpoint string) {
	if tokenCache != nil {
		err := tokenCache.Delete(getCacheKey(username, apiEndpoint))
		if err != nil {
			infolog.Println(fmt.Errorf("could not remove token from the memory cache: %v", err))
		}
	}
}

// isServiceAccount checks if a username is a service account client id
func isServiceAccount(username string) bool {
	return !strings.Contains(username, "@")
}
